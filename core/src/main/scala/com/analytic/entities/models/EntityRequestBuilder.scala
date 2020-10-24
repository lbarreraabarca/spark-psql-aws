package com.analytic.entities.models

import scala.util.Try
import java.time.LocalDate

import scala.collection.mutable.ListBuffer
import com.analytic.entities.utils.CoreUtils._
import com.analytic.entities.models.DataBaseType.DataBaseType

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

  if (!isValidDate) addError(s"invalid processDate [${_processDate}]")
  if (!isValidSourceBucket) addError(s"invalid sourceBucket [${_sourceBucket}]")
  if (!isValidOutputBucket) addError(s"invalid outputBucket [${_outputBucket}]")
  if (!isValidEntityName) addError(s"invalid entityName [${inputRequestFile.entityName}]")
  if (!isValidCountryCode) addError(s"invalid countryCode [${inputRequestFile.countryCode}]")
  if (!isValidRequiredTables) addError(s"invalid requiredTables [${inputRequestFile.requiredTables}]")
  if (!isValidQuery) addError(s"invalid sqlQuery [${inputRequestFile.sqlQuery}]")

  protected lazy val requiredTablesReferences: List[TableReference] = requiredTables.flatMap(toTableReference(_))

  lazy val entityRequest: EntityRequest = EntityRequest(
    entityName,
    countryCode,
    processDate,
    requiredTablesReferences,
    sqlQuery,
    errorList.toList,
  )

  protected def addError(message: String): Unit = errorList += s"RequestException: $message"

  protected def fieldValidator: String => Boolean = f => ("""[^-$`_a-z0-9]""".r findFirstMatchIn f).isEmpty
  protected def tableIdValidator: String => String => Boolean = query => t => fieldValidator(t.toLowerCase) &&
    (("""(?<=\s|^)(?i)""" + t.toLowerCase + """(?=\s|\)|$)""").r findFirstMatchIn query.toLowerCase).isDefined
  protected def requiredTablesValidator: List[_] => Boolean = t => t.nonEmpty && !t.contains(null)

  protected def bigQueryOptionsValidator: Map[String, String] => Boolean = bqOptions =>
    if (bqOptions.contains("clusterBy")) bqOptions("clusterBy").split(",").nonEmpty else true

  protected def dbHostValidator: String => Boolean = x => {
    val regexDbHost = """^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$""".r
    x match {
      case regexDbHost(_*) => true
      case _ => throw new IllegalArgumentException("dbHost do not match with regexHost.")
    }
  }
  protected def dbPortValidator: String => Boolean = x => {
    val regexDbPort = """^[0-9]{1,3}$""".r
    x match {
      case regexDbPort(_*) => true
      case _ => throw new IllegalArgumentException("dbPort do not match with regexDbPort.")
    }
  }
  protected def queryTransformer(implicit pDate: LocalDate): String => String =  query => {
    if (query.nonEmpty) query.replace("<process_date>", pDate.toString)
    else throw new IllegalArgumentException("query cannot be empty")
  }
  protected def processDateTransformer: String => LocalDate = extractDate
  protected def countryCodeTransformer: String => CountryCode = c => CountryCode.withName(c.toUpperCase)
  protected def rdbmsTransformer: String => DataBaseType = l => DataBaseType.withName(l.toUpperCase)
  protected def storageUrlTransformer(implicit pDate: LocalDate): String => String = u =>
    if (("""[^-./_a-z0-9]""".r findFirstMatchIn u.toLowerCase).isEmpty)
      u.toLowerCase.stripPrefix("/").stripSuffix("/") + "/" + pDate.toString.replace("-", "/")
    else throw new IllegalArgumentException("invalid base url")

  protected def getDefaultBqDataSet(implicit c: CountryCode): String = c.toString.toLowerCase + "_entities"
  protected def getDefaultStorageUrl(entity: String)(implicit c: CountryCode, d: LocalDate): String =
    s"$c/${c}_entities/$entity/data/${d.toString.replace("-", "/")}".toLowerCase

  protected def toTableReference(t: InputTableSpec)(implicit pDate: LocalDate): Option[TableReference] = {
    val (tableId, isValidTableId) = validateField(t.tableId, tableIdValidator(sqlQuery), "_")
    val (rdms, isValidRdbms) = transformField(t.rdms, rdbmsTransformer, DataBaseType.NONE)
    val (dbHost, isValidDbHost) = validateField(t.dbHost, dbHostValidator, "_")
    val (dbPort, isValidDbPort) = validateField(t.dbPort, dbPortValidator, "_")
    val (dbName, isValidDbName) = validateField(t.dbName, fieldValidator, "_")
    val (dbUsername, isValidDbUsername) = validateField(t.dbUsername, fieldValidator, "_")
    val (dbPassword, isValidDbPassword) = validateField(t.dbPassword, fieldValidator, "_")
    val (querySource, isValidQuerySource) = validateField(t.querySource, fieldValidator, "_") //TODO

    if (!isValidTableId) { addError(s"tableId invalid or not found in query [${t.tableId}]") }
    if (!isValidRdbms) { addError(s"invalid rdms ${t.rdms}") }
    if (!isValidDbHost) {addError(s"invalid dbHost ${t.dbHost}")}
    if (!isValidDbPort) {addError(s"invalid dbPort ${t.dbPort}")}
    if (!isValidDbName) {addError(s"invalid dbName ${t.dbName}")}
    if (!isValidDbUsername) {addError(s"invalid dbUsername ${t.dbUsername}")}
    if (!isValidDbPassword) {addError(s"invalid dbPassword ${t.dbPassword}")}
    if (!isValidQuerySource) {addError(s"invalid querySource ${t.querySource}")}

    Try {
      TableReference(
        tableId = tableId,
        rdms = rdms,
        dbHost = dbHost,
        dbPort = dbPort,
        dbName = dbName,
        dbUsername = dbUsername,
        dbPassword = dbPassword,
        querySource = querySource,
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