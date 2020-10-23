package com.analytic.entities.models

import scala.util.Try
import java.time.LocalDate
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import com.analytic.entities.utils.CoreUtils._

case class EntityRequestBuilder (
  inputRequestFile: InputEntityRequest,
  _processDate: String,
  _sourceBucket: String,
  _outputBucket: String,
) {
  import CountryCode.CountryCode, TableLoadType.TableLoadType

  val errorList: ListBuffer[String] = ListBuffer()
  implicit lazy val iProcessDate: LocalDate = processDate
  implicit lazy val iCountryCode: CountryCode = countryCode

  protected lazy val DefaultHostBucket: String = sourceBucket
  protected lazy val DefaultLoadType: TableLoadType = TableLoadType.INTERVAL
  protected lazy val DefaultPartitionsDays = 1
  protected lazy val DefaultPartitionDelay = 0
  protected lazy val DefaultBqDataSet: String = getDefaultBqDataSet
  protected lazy val DefaultStorageUrl: String = getDefaultStorageUrl(entityName)

  protected val SchemaRequiredFormats: List[String] = List("tsv", "csv")
  protected val ValidFileFormats: List[String] = List("avro", "bigquery") ++ SchemaRequiredFormats

  protected val (processDate, isValidDate): (LocalDate, Boolean) =
    transformField(_processDate, processDateTransformer, LocalDate.of(1,1,1))
  protected val (sourceBucket, isValidSourceBucket) = validateField(_sourceBucket, fieldValidator, "_")
  protected val (outputBucket, isValidOutputBucket) = validateField(_outputBucket, fieldValidator, "_")
  protected val (entityName, isValidEntityName) = validateField(inputRequestFile.entityName, fieldValidator, "_")
  protected val (countryCode, isValidCountryCode) = transformField(inputRequestFile.countryCode, countryCodeTransformer, CountryCode.NONE)
  protected val (requiredTables, isValidRequiredTables) = validateField(inputRequestFile.requiredTables, requiredTablesValidator, Nil)
  protected val (sqlQuery, isValidQuery) = transformField(inputRequestFile.sqlQuery, queryTransformer, "_")
  protected val (storageUrl, isValidStorageUrl) = transformField(inputRequestFile.storageUrl, storageUrlTransformer, DefaultStorageUrl)
  protected val (bqDataSet, isValidBqDataset) = validateField(inputRequestFile.bqDataset, fieldValidator, DefaultBqDataSet)
  protected val (bqOptions, isValidBqOptions) = validateField(inputRequestFile.bqOptions, bigQueryOptionsValidator, Map.empty[String, String])

  if (!isValidDate) addError(s"invalid processDate [${_processDate}]")
  if (!isValidSourceBucket) addError(s"invalid sourceBucket [${_sourceBucket}]")
  if (!isValidOutputBucket) addError(s"invalid outputBucket [${_outputBucket}]")
  if (!isValidEntityName) addError(s"invalid entityName [${inputRequestFile.entityName}]")
  if (!isValidCountryCode) addError(s"invalid countryCode [${inputRequestFile.countryCode}]")
  if (!isValidRequiredTables) addError(s"invalid requiredTables [${inputRequestFile.requiredTables}]")
  if (!isValidQuery) addError(s"invalid sqlQuery [${inputRequestFile.sqlQuery}]")
  if (!isValidStorageUrl) addError(s"invalid storageUrl [${inputRequestFile.storageUrl.getOrElse("")}]")
  if (!isValidBqDataset) addError(s"invalid bqDataset [${inputRequestFile.bqDataset.getOrElse("")}]")
  if (!isValidBqOptions) addError(s"invalid bqOptions [${inputRequestFile.bqDataset.getOrElse("")}]")

  protected lazy val entityTable: TablePartition = TablePartition(entityName, storageUrl, outputBucket)
  protected lazy val requiredTablesReferences: List[TableReference] = requiredTables.flatMap(toTableReference(_))

  lazy val entityRequest: EntityRequest = EntityRequest(
    entityTable,
    bqDataSet,
    processDate,
    requiredTablesReferences,
    sqlQuery,
    errorList.toList,
    bqOptions
  )

  protected def addError(message: String): Unit = errorList += s"RequestException: $message"

  protected def fieldValidator: String => Boolean = f => ("""[^-$`_a-z0-9]""".r findFirstMatchIn f).isEmpty
  protected def tableIdValidator: String => String => Boolean = query => t => fieldValidator(t.toLowerCase) &&
    (("""(?<=\s|^)(?i)""" + t.toLowerCase + """(?=\s|\)|$)""").r findFirstMatchIn query.toLowerCase).isDefined
  protected def gcsUrlValidator: String => Boolean = url => ("""^gs://[-._/a-z0-9]+$""".r findFirstMatchIn url).isDefined
  protected def requiredTablesValidator: List[_] => Boolean = t => t.nonEmpty && !t.contains(null)
  protected def partitionsDaysValidator: Int => Boolean = _ > 0
  protected def partitionDelayValidator: Int => Boolean = _ >= 0

  protected def bigQueryOptionsValidator: Map[String, String] => Boolean = bqOptions =>
    if (bqOptions.contains("clusterBy")) bqOptions("clusterBy").split(",").nonEmpty else true

  protected def urlRgxTransformer: String => Regex = x => if (x.contains("/*.")) {
    ("^(" + x.toLowerCase.stripPrefix("/")
      .replace("/*.", """)/*.""")
      .replace(".", """\.""")
      .replace("/*", """/[-._a-z0-9]+""")
      .replace("/<process_date>", """/\d{4}/\d{2}/\d{2}""")
      + "$"
      ).r
  } else throw new IllegalArgumentException("invalid url")

  protected def urlStemTransformer: String => String = x => {
    val u = x.stripPrefix("/").toLowerCase
    """/[<*]""".r findFirstIn u match {
      case Some(m) => u take u.indexOfSlice(m)
      case None => throw new IllegalArgumentException("unable to find stem in url")
    }
  }
  protected def urlFileFormatTransformer: Seq[String] => String => String = (validFormats: Seq[String]) => u => {
    val format = u.split("""\.""").last.toLowerCase
    if (validFormats contains format) format else throw new IllegalArgumentException("invalid file format")
  }
  protected def queryTransformer(implicit pDate: LocalDate): String => String =  query => {
    if (query.nonEmpty) query.replace("<process_date>", pDate.toString)
    else throw new IllegalArgumentException("query cannot be empty")
  }
  protected def processDateTransformer: String => LocalDate = extractDate
  protected def countryCodeTransformer: String => CountryCode = c => CountryCode.withName(c.toUpperCase)
  protected def loadTypeTransformer: String => TableLoadType = l => TableLoadType.withName(l.toUpperCase)
  protected def storageUrlTransformer(implicit pDate: LocalDate): String => String = u =>
    if (("""[^-./_a-z0-9]""".r findFirstMatchIn u.toLowerCase).isEmpty)
      u.toLowerCase.stripPrefix("/").stripSuffix("/") + "/" + pDate.toString.replace("-", "/")
    else throw new IllegalArgumentException("invalid base url")

  protected def getDefaultBqDataSet(implicit c: CountryCode): String = c.toString.toLowerCase + "_entities"
  protected def getDefaultStorageUrl(entity: String)(implicit c: CountryCode, d: LocalDate): String =
    s"$c/${c}_entities/$entity/data/${d.toString.replace("-", "/")}".toLowerCase

  protected def toTableReference(t: InputTableSpec)(implicit pDate: LocalDate): Option[TableReference] = {
    val (tableId, isValidTableId) = validateField(t.tableId, tableIdValidator(sqlQuery), "_")
    val (rgxUrl, isValidUrlA) = transformField(t.coreUrl, urlRgxTransformer, "_".r)
    val (stemUrl, isValidUrlB) = transformField(t.coreUrl, urlStemTransformer, "_")
    val (fileFormat, isValidUrlC) = transformField(t.coreUrl, urlFileFormatTransformer(ValidFileFormats), "_")
    val (hostBkt, isValidHostBucket) = validateField(t.hostBucket, fieldValidator, DefaultHostBucket)
    val (loadType, isValidLoadType) = transformField(t.loadType, loadTypeTransformer, DefaultLoadType)
    val (schemaFile, isValidSchemaFileUrl) = validateField(t.schemaFile, gcsUrlValidator, "_")
    val (partitionsDays, isValidPartitionDays) = validateField(t.partitionsDays, partitionsDaysValidator, DefaultPartitionsDays)
    val (partitionDelay, isValidPartitionDelay) = validateField(t.partitionDelay, partitionDelayValidator, DefaultPartitionDelay)

    if (!isValidTableId) { addError(s"tableId invalid or not found in query [${t.tableId}]") }
    if (!isValidUrlA || !isValidUrlB || !isValidUrlC) { addError(s"invalid table coreUrl for tableId '${t.tableId}' [${t.coreUrl}]") }
    if (SchemaRequiredFormats.contains(fileFormat) && (schemaFile == "_" || !isValidSchemaFileUrl)) {
      addError(s"invalid schemaFile for $fileFormat file format for tableId '${t.tableId}' [${t.schemaFile.getOrElse("")}]")
    }
    if (!isValidHostBucket) { addError(s"invalid table hostBucket for tableId '${t.tableId}' [${t.hostBucket.getOrElse("")}]") }
    if (!isValidLoadType) { addError(s"invalid table loadType for tableId '${t.tableId}' [${t.loadType.getOrElse("")}]") }
    if (!isValidPartitionDays) { addError(s"invalid table partitionsDays for tableId '${t.tableId}' [${t.partitionsDays.getOrElse("")}]") }
    if (!isValidPartitionDelay) { addError(s"invalid table partitionDelay for tableId '${t.tableId}' [${t.partitionDelay.getOrElse("")}]") }
    Try {
      TableReference(
        tableId = tableId,
        regexUrl = rgxUrl,
        stemUrl = stemUrl,
        processDate = pDate,
        hostBucket = hostBkt,
        fileFormat = fileFormat,
        schemaSource = if (schemaFile == "_") None else Some(schemaFile),
        loadType = loadType,
        partitionsDays = partitionsDays,
        partitionDelay = partitionDelay
      )
    }.toOption
  }

}
object EntityRequestBuilder {
  def apply(
    inputRequestFile: InputEntityRequest,
    _processDate: String,
    _sourceBucket: String,
    _outputBucket: String): EntityRequest = {
    val builder = new EntityRequestBuilder(inputRequestFile, _processDate, _sourceBucket, _outputBucket)
    builder.entityRequest
  }
}