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
package com.snowplowanalytics
package snowplow.enrich
package spark

// Java
import java.net.URI

// jackson
import com.fasterxml.jackson.databind.JsonNode

// Joda
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Scopt
import scopt._

// Snowplow
import common.{ValidatedMessage, ValidatedNelMessage}
import common.enrichments.EnrichmentRegistry
import common.loaders.Loader
import common.utils.{ConversionUtils, JsonUtils}
import iglu.client.Resolver
import iglu.client.validation.ProcessingMessageMethods._

sealed trait EnrichJobConfig {
  def inFolder: String
  def outFolder: String
  def badFolder: String
}

private case class RawEnrichJobConfig(
  override val inFolder: String = "",
  override val outFolder: String = "",
  override val badFolder: String = "",
  enrichments: String = "",
  inFormat: String = "",
  igluConfig: String = "",
  etlTstamp: Long = 0L
) extends EnrichJobConfig

/**
 * Case class representing the configuration for the enrich job.
 * @param inFolder Folder where the input events are located
 * @param outFolder Output folder where the enriched events will be stored
 * @param badFolder Output folder where the malformed events will be stored
 * @param igluResolver Iglu resolver used for schema lookups in the job
 * @param registry Registry of enrichments
 * @param loader Loader associated with the specified collector identifier
 * @param etlTstamp Timestamp at which the job was launched
 */
case class ParsedEnrichJobConfig(
  override val inFolder: String,
  override val outFolder: String,
  override val badFolder: String,
  igluResolver: Resolver,
  registry: EnrichmentRegistry,
  loader: Loader[_],
  etlTstamp: DateTime,
  filesToCache: List[(URI, String)]
) extends EnrichJobConfig

object EnrichJobConfig {
  private val parser = new scopt.OptionParser[RawEnrichJobConfig]("EnrichJob") {
    head("EnrichJob")

    opt[String]("input-folder").required().valueName("<input folder>")
      .action((f, c) => c.copy(inFolder = f))
      .text("Folder where the input events are located")
    opt[String]("input-format").required().valueName("<input format>")
      .action((f, c) => c.copy(inFormat = f))
      .text("The format in which the collector is saving data")
    opt[String]("output-folder").required().valueName("<output folder>")
      .action((f, c) => c.copy(outFolder = f))
      .text("Output folder where the enriched events will be stored")
    opt[String]("bad-folder").required().valueName("<bad folder>")
      .action((f, c) => c.copy(badFolder = f))
      .text("Output folder where the malformed events will be stored")
    opt[String]("enrichments").required().valueName("<enrichments>")
      .action((e, c) => c.copy(enrichments = e))
      .text("JSONs describing the enrichments")
    opt[String]("iglu-config").required().valueName("<iglu config>")
      .action((i, c) => c.copy(igluConfig = i))
      .text("Iglu configuration")
    opt[Long]("etl-timestamp").required().valueName("<ETL timestamp>")
      .action((t, c) => c.copy(etlTstamp = t))
      .text("Timestamp at which the job was launched, in Epoch time")
    help("help").text("Prints this usage text")
  }

  /**
   * Load a EnrichJobConfig from command line arguments.
   * @param args The command line arguments
   * @return The job config or one or more error messages boxed in a Scalaz ValidationNel
   */
  def loadConfigFrom(
    args: Array[String]
  ): ValidatedNelMessage[ParsedEnrichJobConfig] = {
    parser.parse(args, RawEnrichJobConfig()) match {
      case Some(c) =>
        val resolver = getIgluResolver(c.igluConfig)
        val registry = resolver.flatMap(getEnrichmentRegistry(c.enrichments)(_))
        val loader = Loader.getLoader(c.inFormat)
          .fold(_.toProcessingMessage.failureNel, _.successNel)
        (resolver |@| registry |@| loader) { (res, reg, l) =>
            ParsedEnrichJobConfig(c.inFolder, c.outFolder, c.badFolder,
              res, reg, l, new DateTime(c.etlTstamp), filesToCache(reg))
        }
      case None => "Parsing of the configuration failed".toProcessingMessage.failureNel
    }
  }

  /**
   * Build the list of enrichment files to cache.
   * @param enrichments JSONs describing the enrichments that need performing
   * @param resolver Iglu resolver used to lookup schemas
   * @return A list of (URI, String) pairs representing the files that need caching
   */
  private def filesToCache(registry: EnrichmentRegistry): List[(URI, String)] =
    registry.getIpLookupsEnrichment match {
      case Some(ipLookups) =>
        ipLookups.dbsToCache.map { case (uri, _) => (uri, extractFilenameFromUri(uri)) }
      case None => Nil
    }

  /**
   * Build an EnrichmentRegistry from the enrichments arg.
   * @param enrichments The JSON of all enrichments constructed by EmrEtlRunner
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation
   * @return An EnrichmentRegistry or one or more error messages boxed in a Scalaz ValidationNel
   */
  private def getEnrichmentRegistry(enrichments: String
  )(implicit resolver: Resolver): ValidatedNelMessage[EnrichmentRegistry] = {
    import org.json4s.jackson.JsonMethods.fromJsonNode
    for {
      node <- base64ToJsonNode(enrichments, "enrichments")
        .toValidationNel: ValidatedNelMessage[JsonNode]
      reg <- EnrichmentRegistry.parse(fromJsonNode(node), false)
    } yield reg
  }

  /**
   * Build an Iglu resolver from a JSON.
   * @param json JSON representing the Iglu resolver
   * @return A Resolver or one or more error messages boxed in a Scalaz ValidationNel
   */
  private def getIgluResolver(json: String): ValidatedNelMessage[Resolver] =
    for {
      node <- (base64ToJsonNode(json, "iglu").toValidationNel: ValidatedNelMessage[JsonNode])
      reso <- Resolver.parse(node)
    } yield reso

  /**
   * Convert a base64-encoded JSON String into a JsonNode.
   * @param str base64-encoded JSON
   * @param field name of the field to be decoded
   * @return a JsonNode on Success, a NonEmptyList of ProcessingMessages on Failure
   */
  private def base64ToJsonNode(str: String, field: String): ValidatedMessage[JsonNode] =
    (for {
      raw  <- ConversionUtils.decodeBase64Url(field, str)
      node <- JsonUtils.extractJson(field, raw)
    } yield node).toProcessingMessage

  /**
   * A helper to get the filename from a URI.
   * @param uri The URL to extract the filename from
   * @return The extracted filename
   */
  private def extractFilenameFromUri(uri: URI): String = {
    val p = uri.getPath
    p.substring(p.lastIndexOf('/') + 1, p.length)
  }
}
