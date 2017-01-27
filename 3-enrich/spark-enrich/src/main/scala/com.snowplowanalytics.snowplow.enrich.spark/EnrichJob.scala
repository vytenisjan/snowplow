/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package spark

// Apache commons
import org.apache.commons.codec.binary.Base64

// Scalaz
import scalaz._
import Scalaz._

// Spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Encoders, SparkSession}

// Snowplow
import common.{EtlPipeline, FatalEtlError, ValidatedEnrichedEvent}
import common.loaders.Loader
import common.outputs.{BadRow, EnrichedEvent}

object EnrichJob extends SparkJob {
  private[spark] val classesToRegister: Array[Class[_]] = Array.empty
  override def sparkConfig(): SparkConf = new SparkConf()
    .setAppName(getClass().getSimpleName())
    .setIfMissing("spark.master", "local[*]")
    .set("spark.serializer", classOf[KryoSerializer].getName())
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(classesToRegister)

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    val job = EnrichJob(spark, args)
    job.run()
  }

  def apply(spark: SparkSession, args: Array[String]) = new EnrichJob(spark, args)

  val etlVersion = s"spark-${generated.ProjectSettings.version}"

  /**
   * Project our Failures into a List of Nel of strings.
   * @param all A List of Validations each containing either an EnrichedEvent or Failure strings
   * @return A (possibly empty) List of failures, where each failure is a Nel of strings
   */
  def projectBads(all: List[ValidatedEnrichedEvent]): List[NonEmptyList[String]] =
    all.collect { case Failure(errs) => errs }

  /**
   * Project our Sucesses into EnrichedEvents.
   * @param all A List of Validations each containing either an EnrichedEvent or Failure strings
   * @return A (possibly empty) List of EnrichedEvent
   */
  def projectGoods(all: List[ValidatedEnrichedEvent]): List[EnrichedEvent] =
    all.collect { case Success(e) => e }
}

/**
 * The Snowplow Enrich job, written in Spark.
 * @param spark Spark session used throughout the job
 * @param args Command line arguments for the enrich job
 */
class EnrichJob(@transient val spark: SparkSession, args: Array[String]) {
  @transient private val sc: SparkContext = spark.sparkContext
  import spark.implicits._

  // Job configuration
  private val enrichConfig = EnrichJobConfig.loadConfigFrom(args).fold(
    e => throw FatalEtlError(e.map(_.toString)),
    identity
  )

  def run(): Unit = {
    import EnrichJob._

    implicit val resolver = enrichConfig.igluResolver
    val loader = enrichConfig.loader.asInstanceOf[Loader[Any]]
    val registry = enrichConfig.registry

    // Install MaxMind file(s) if we have them
    enrichConfig.filesToCache.foreach { case (uri, filename) => sc.addFile(uri.toString) }

    val input = getInputRDD(enrichConfig.loader, enrichConfig.inFolder)

    val common = input
      .map { line =>
        (line, EtlPipeline.processEvents(registry, etlVersion, enrichConfig.etlTstamp,
          loader.toCollectorPayload(line)))
      }
      .cache()

    // Handling of malformed rows
    val bad = common
      .map { case (line, enriched) => (line, projectBads(enriched)) }
      .flatMap { case (line, errors) =>
        val originalLine = line match {
          case bytes: Array[Byte] => new String(Base64.encodeBase64(bytes), "UTF-8")
          case other => other.toString
        }
        errors.map(BadRow(originalLine, _).toCompactJson)
      }
    if (!bad.isEmpty) bad.saveAsTextFile(enrichConfig.badFolder)

    // Handling of properly-formed rows
    val good = common
      .flatMap { case (line, enriched) => projectGoods(enriched) }
    if (!good.isEmpty)
      spark.createDataset(good)(Encoders.bean(classOf[EnrichedEvent]))
        .write
        .option("sep", "\t")
        .csv(enrichConfig.outFolder)
  }

  private def getInputRDD(loader: Loader[_], path: String): RDD[_] = {
    val `Loader[Array[Byte]]` = shapeless.TypeCase[Loader[Array[Byte]]]
    val `Loader[String]` = shapeless.TypeCase[Loader[String]]
    enrichConfig.loader match {
      case `Loader[Array[Byte]]`(_) =>
        sc.newAPIHadoopFile(enrichConfig.inFolder,
          classOf[com.hadoop.mapreduce.LzoTextInputFormat],
          classOf[org.apache.hadoop.io.LongWritable],
          classOf[org.apache.hadoop.io.Text]
        ).map(_._2.getBytes())
      case `Loader[String]`(_) => sc.textFile(enrichConfig.inFolder)
      case _ => throw FatalEtlError("Loader not supported")
    }
  }
}
