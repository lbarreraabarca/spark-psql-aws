package com.analytic.entities.models

import java.time.LocalDate

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}

import scala.util.matching.Regex

class EntityRequestBuilderTest extends AnyFlatSpec with PrivateMethodTester with BeforeAndAfterEach {
  import CountryCode.CountryCode, TableLoadType.TableLoadType
  var testBuilder: EntityRequestBuilder = _

  def addError(m: String): Unit = testBuilder invokePrivate PrivateMethod[Unit]('addError)(m)

  def getDefaultBqDataSet(c: CountryCode): String = testBuilder invokePrivate PrivateMethod[String]('getDefaultBqDataSet)(c)
  def getDefaultStorageUrl(s: String)(c: CountryCode, d: LocalDate): String = testBuilder invokePrivate PrivateMethod[String]('getDefaultStorageUrl)(s, c, d)

  def fieldValidator: String => Boolean = testBuilder invokePrivate PrivateMethod[String => Boolean]('fieldValidator)()
  def tableIdValidator: String => String => Boolean = testBuilder invokePrivate PrivateMethod[String => String => Boolean]('tableIdValidator)()
  def gcsUrlValidator: String => Boolean = testBuilder invokePrivate PrivateMethod[String => Boolean]('gcsUrlValidator)()
  def requiredTablesValidator: List[_] => Boolean = testBuilder invokePrivate PrivateMethod[List[_] => Boolean]('requiredTablesValidator)()
  def partitionsDaysValidator: Int => Boolean = testBuilder invokePrivate PrivateMethod[Int => Boolean]('partitionsDaysValidator)()
  def partitionDelayValidator: Int => Boolean = testBuilder invokePrivate PrivateMethod[Int => Boolean]('partitionDelayValidator)()
  def bigQueryOptionsValidator: Map[String, String] => Boolean = testBuilder invokePrivate PrivateMethod[Map[String, String] => Boolean]('bigQueryOptionsValidator)()
  def urlRgxTransformer: String => Regex = testBuilder invokePrivate PrivateMethod[String => Regex]('urlRgxTransformer)()
  def urlStemTransformer: String => String = testBuilder invokePrivate PrivateMethod[String => String]('urlStemTransformer)()
  def urlFileFormatTransformer: Seq[String] => String => String = testBuilder invokePrivate PrivateMethod[Seq[String] => String => String]('urlFileFormatTransformer)()
  def queryTransformer(d: LocalDate): String => String = testBuilder invokePrivate PrivateMethod[String => String]('queryTransformer)(d)
  def processDateTransformer: String => LocalDate = testBuilder invokePrivate PrivateMethod[String => LocalDate]('processDateTransformer)()
  def countryCodeTransformer: String => CountryCode = testBuilder invokePrivate PrivateMethod[String => CountryCode]('countryCodeTransformer)()
  def loadTypeTransformer: String => TableLoadType = testBuilder invokePrivate PrivateMethod[String => TableLoadType]('loadTypeTransformer)()
  def storageUrlTransformer(d: LocalDate): String => String = testBuilder invokePrivate PrivateMethod[String => String]('storageUrlTransformer)(d)

  def toTableReference(t: InputTableSpec)(date: LocalDate): Option[TableReference] = testBuilder invokePrivate PrivateMethod[Option[TableReference]]('toTableReference)(t, date)

  override def beforeEach(): Unit = {
    // Valid entry point instance
    testBuilder = new EntityRequestBuilder(
      inputRequestFile = InputEntityRequest(
        entityName = "test_entity",
        countryCode = "ar",
        requiredTables = List(InputTableSpec("test_table", "test_table/<process_date>/*.avro")),
        sqlQuery = "SELECT * from test_table t1 where t1.date = <process_date>"
      ),
      _processDate = "2000-01-01",
      _sourceBucket = "src-bkt",
      _outputBucket = "out-bkt"
    )
    assert(testBuilder.entityRequest.errorList.isEmpty)
  }

  "addError" should "add an Exception message to errorList" in {
    assert(testBuilder.errorList.isEmpty)
    addError("an error occurred")
    addError("another error occurred")
    assert(testBuilder.errorList.nonEmpty)
    val actual = testBuilder.errorList.mkString(", ")
    val expected = "RequestException: an error occurred, RequestException: another error occurred"
    assertResult(expected)(actual)
  }

  "getDefaultBqDataSet" should "return the correct default dataset" in {
    val actual = getDefaultBqDataSet(CountryCode.AR)
    val expected = "ar_entities"
    assertResult(expected)(actual)
  }

  "getDefaultStorageUrl" should "return the correct default dataset" in {
    val actual = getDefaultStorageUrl("my-entity")(CountryCode.AR, LocalDate.of(1,1,1))
    val expected = "ar/ar_entities/my-entity/data/0001/01/01"
    assertResult(expected)(actual)
  }

  "fieldValidator" should "return true on valid input" in {
    val actual = fieldValidator("some_input24")
    assert(actual)
  }
  it should "return false on invalid input" in {
    val actual = fieldValidator("some other input")
    assert(!actual)
  }

  "tableIdValidator" should "return true on valid tableId" in {
    val actual = tableIdValidator("select * from valid__tableId")("valid__tableId")
    assert(actual)
  }
  it should "return false on missing reference in query" in {
    val actual = tableIdValidator("select * from one__tableId")("missing__tableId")
    assert(!actual)
  }
  it should "return false on invalid tableId" in {
    val actual = tableIdValidator("select * from invalid.tableId")("invalid.tableId")
    assert(!actual)
  }

  "gcsUrlValidator" should "return true on valid gcs url" in {
    val actual = gcsUrlValidator("gs://valid/schema/source.json")
    assert(actual)
  }
  it should "return false on invalid gcs url" in {
    val actual = gcsUrlValidator("invalid/gcs/url")
    assert(!actual)
  }

  "requiredTablesValidator" should "return true on valid List of TableSpec" in {
    val actual = requiredTablesValidator(List(InputTableSpec("test", "url/*.avro")))
    assert(actual)
  }
  it should "return false on invalid List of TableSpec" in {
    val actual = requiredTablesValidator(List(null, InputTableSpec("test", "url/*.avro")))
    assert(!actual)
  }

  "partitionDaysValidator" should "return true with 3 as input" in {
    val actual = partitionsDaysValidator(3)
    assert(actual)
  }
  it should "return true with 1 as input" in {
    val actual = partitionsDaysValidator(1)
    assert(actual)
  }
  it should "return false with 0 as input" in {
    val actual = partitionsDaysValidator(0)
    assert(!actual)
  }
  it should "return false with negative input" in {
    val actual = partitionsDaysValidator(-2)
    assert(!actual)
  }

  "partitionDelayValidator" should "return true with 3 as input" in {
    val actual = partitionDelayValidator(3)
    assert(actual)
  }
  it should "return true with 0 as input" in {
    val actual = partitionDelayValidator(0)
    assert(actual)
  }
  it should "return false with negative input" in {
    val actual = partitionDelayValidator(-2)
    assert(!actual)
  }

  "bigQueryOptionsValidator" should "return true with a valid list." in {
    val actual = bigQueryOptionsValidator(Map("clusterBy"-> "FIELD1,FIELD2"))
    assert(actual)
  }
  /*it should "return false with a list contains null values." in {
    val actual = bigQueryOptionsValidator(Map.empty[String, String])
    assert(!actual)
  }*/

  "urlRgxTransformer" should "return the correct regex [case 1]" in {
    val actual = urlRgxTransformer("this/is/an/url/<process_date>/data/*.avro")
    val expected = """^(this/is/an/url/\d{4}/\d{2}/\d{2}/data)/[-._a-z0-9]+\.avro$""".r
    assertResult(expected.toString)(actual.toString)
  }
  it should "return the correct regex [case 2]" in {
    val actual = urlRgxTransformer("another/url/<process_date>/*/*/*.tsv")
    val expected = """^(another/url/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+/[-._a-z0-9]+)/[-._a-z0-9]+\.tsv$""".r
    assertResult(expected.toString)(actual.toString)
  }
  it should "return the correct regex [case 3]" in {
    val actual = urlRgxTransformer("simple/*.csv")
    val expected = """^(simple)/[-._a-z0-9]+\.csv$""".r
    assertResult(expected.toString)(actual.toString)
  }
  it should "throw IllegalArgumentException on invalid input" in {
    assertThrows[IllegalArgumentException]{
      urlRgxTransformer("invalid/core/url.avro")
    }
  }

  "urlStemTransformer" should "return the correct url stem [case 1]" in {
    val actual = urlStemTransformer("this/is/an/url/<process_date>/data/*.avro")
    val expected = "this/is/an/url"
    assertResult(expected)(actual)
  }
  it should "return the correct url stem [case 2]" in {
    val actual = urlStemTransformer("another/url/<process_date>/*/*/*.tsv")
    val expected = "another/url"
    assertResult(expected)(actual)
  }
  it should "return the correct url stem [case 3]" in {
    val actual = urlStemTransformer("simple/*.csv")
    val expected = "simple"
    assertResult(expected)(actual)
  }
  it should "return the correct url stem [case 4]" in {
    val actual = urlStemTransformer("some_dataset.table_x/*.bigquery")
    val expected = "some_dataset.table_x"
    assertResult(expected)(actual)
  }
  it should "throw IllegalArgumentException on invalid input" in {
    assertThrows[IllegalArgumentException]{
      urlStemTransformer("invalid/core/url.avro")
    }
  }

  "urlFileFormatTransformer" should "return the correct format [case 1]" in {
    val actual = urlFileFormatTransformer(List("avro", "tsv", "csv"))("this/is/an/url/<process_date>/data/*.avro")
    val expected = "avro"
    assertResult(expected)(actual)
  }
  it should "return the correct format [case 2]" in {
    val actual = urlFileFormatTransformer(List("avro", "tsv", "csv"))("another/url/<process_date>/*/*/*.tsv")
    val expected = "tsv"
    assertResult(expected)(actual)
  }
  it should "return the correct format [case 3]" in {
    val actual = urlFileFormatTransformer(List("avro", "tsv", "csv"))("simple/*.csv")
    val expected = "csv"
    assertResult(expected)(actual)
  }
  it should "return the correct format [case 4]" in {
    val actual = urlFileFormatTransformer(List("avro", "tsv", "csv", "bigquery"))("simple$`_PARTITION`/*.bigquery")
    val expected = "bigquery"
    assertResult(expected)(actual)
  }
  it should "throw IllegalArgumentException on invalid input" in {
    assertThrows[IllegalArgumentException]{
      urlFileFormatTransformer(List("avro", "tsv", "csv"))("invalid/core/url.mp4")
    }
  }

  "queryTransformer" should "return the query with the necessary replacements" in {
    val actual = queryTransformer(LocalDate.of(1,1,1))("SELECT * FROM table WHERE dt = <process_date>")
    val expected = "SELECT * FROM table WHERE dt = 0001-01-01"
    assertResult(expected)(actual)
  }
  it should "throw IllegalArgumentException on empty query" in {
    assertThrows[IllegalArgumentException]{
      queryTransformer(LocalDate.of(1, 1, 1))("")
    }
  }

  "processDateTransformer" should "return the correct LocalDate" in {
    val actual = processDateTransformer("2020-12-24")
    val expected = LocalDate.of(2020,12,24)
    assertResult(expected)(actual)
  }

  "countryCodeTransformer" should "return the correct CountryCode" in {
    val actual = countryCodeTransformer("Co")
    val expected =  CountryCode.CO
    assertResult(expected)(actual)
  }

  "loadTypeTransformer" should "return the correct TableLoadType" in {
    val actual = loadTypeTransformer("Most_Recent")
    val expected = TableLoadType.MOST_RECENT
    assertResult(expected)(actual)
  }

  "storageUrlTransformer" should "return a corrected url" in {
    val actual = storageUrlTransformer(LocalDate.of(2000,12,24))("/path/to/output")
    val expected =  "path/to/output/2000/12/24"
    assertResult(expected)(actual)
  }
  it should "throw IllegalArgumentException on invalid path" in {
    assertThrows[IllegalArgumentException] {
      storageUrlTransformer(LocalDate.of(1,1,1))("gs://invalid/output/path")
    }
  }

  "toTableReference" should "return a valid TableReference with default values on minimal valid InputTableSpec, "+
    "and builder errorList should remain empty" in {
    val actual = Seq(
      InputTableSpec("test_table", "test_table/data/<process_date>/*/*.avro")
    ).flatMap(toTableReference(_)(LocalDate.of(1,1,1)))
    val expected = TableReference(
      tableId = "test_table",
      regexUrl = """^(test_table/data/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+)/[-._a-z0-9]+\.avro$""".r,
      stemUrl = "test_table/data",
      processDate = LocalDate.of(1, 1, 1),
      hostBucket = "src-bkt",
      fileFormat = "avro",
      schemaSource = None,
      loadType = TableLoadType.INTERVAL,
      partitionsDays = 1,
      partitionDelay = 0
    )
    assert(testBuilder.errorList.isEmpty)
    assertResult(expected)(actual.head)
  }
  it should "return a valid TableReference with default values on non minimal valid InputTableSpec, "+
    "and builder errorList should remain empty" in {
    val actual = Seq(
      InputTableSpec(
        tableId = "test_table",
        coreUrl = "test_table/data/<process_date>/*/*.tsv",
        schemaFile = Some("gs://path/to/schema.json"),
        hostBucket = Some("another-host-bkt"),
        loadType = Some("MOST_RECENT_UNTIL"),
        partitionsDays = Some(30),
        partitionDelay = Some(14),
      )
    ).flatMap(toTableReference(_)(LocalDate.of(2000,1,1)))
    val expected = TableReference(
      tableId = "test_table",
      regexUrl = """^(test_table/data/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+)/[-._a-z0-9]+\.tsv$""".r,
      stemUrl = "test_table/data",
      processDate = LocalDate.of(2000, 1, 1),
      hostBucket = "another-host-bkt",
      fileFormat = "tsv",
      schemaSource = Some("gs://path/to/schema.json"),
      loadType = TableLoadType.MOST_RECENT_UNTIL,
      partitionsDays = 30,
      partitionDelay = 14
    )
    assert(testBuilder.errorList.isEmpty)
    assertResult(expected)(actual.head)
  }
  it should "return an invalid TableReference on invalid InputTableSpec with a nonEmpty errorList" in {
    val actualErrorList = testBuilder.errorList
    assert(actualErrorList.isEmpty)

    val actual = Seq(
      InputTableSpec(
        tableId = "invalid/table/id",
        coreUrl = "invalid/file.avro",
        hostBucket = Some("gs://invalid-bkt"),
        loadType = Some("any_load_type"),
        partitionsDays = Some(0),
        partitionDelay = Some(-1)
      )
    ).flatMap(toTableReference(_)(LocalDate.of(2000,1,1)))
    val expected = TableReference(
      tableId = "_",
      regexUrl = """_""".r,
      stemUrl = "_",
      processDate = LocalDate.of(2000, 1, 1),
      hostBucket = "src-bkt",
      fileFormat = "avro",
      schemaSource = None,
      loadType = TableLoadType.INTERVAL,
    )
    val expectedErrorList = List(
      "RequestException: tableId invalid or not found in query [invalid/table/id]",
      "RequestException: invalid table coreUrl for tableId 'invalid/table/id' [invalid/file.avro]",
      "RequestException: invalid table hostBucket for tableId 'invalid/table/id' [gs://invalid-bkt]",
      "RequestException: invalid table loadType for tableId 'invalid/table/id' [any_load_type]",
      "RequestException: invalid table partitionsDays for tableId 'invalid/table/id' [0]",
      "RequestException: invalid table partitionDelay for tableId 'invalid/table/id' [-1]",
    )

    assertResult(expectedErrorList)(actualErrorList)
    assertResult(expected)(actual.head)
  }
  it should "return an invalid TableReference on missing schemaSource for a non avro file format a nonEmpty errorList" in {
    val actualErrorList = testBuilder.errorList
    assert(actualErrorList.isEmpty)

    val actual = Seq(
      InputTableSpec(
        tableId = "test_table",
        coreUrl = "valid/url/*.tsv",
        hostBucket = Some("valid-bkt"),
        loadType = Some("ALL_DATA"),
      )
    ).flatMap(toTableReference(_)(LocalDate.of(2000,1,1)))
    val expected = TableReference(
      tableId = "test_table",
      regexUrl = """^(valid/url)/[-._a-z0-9]+\.tsv$""".r,
      stemUrl = "valid/url",
      processDate = LocalDate.of(2000, 1, 1),
      hostBucket = "valid-bkt",
      fileFormat = "tsv",
      schemaSource = None,
      loadType = TableLoadType.ALL_DATA,
    )
    val expectedErrorList = List(
      "RequestException: invalid schemaFile for tsv file format for tableId 'test_table' []",
    )

    assertResult(expectedErrorList)(actualErrorList)
    assertResult(expected)(actual.head)
  }


  "Instantiation" should "produce an EntityRequest with an empty errorList on valid minimal InputEntityRequest" in {
    val actual = EntityRequestBuilder(
      inputRequestFile = InputEntityRequest(
        entityName = "test_entity",
        countryCode = "pe",
        requiredTables = List(InputTableSpec("test_table", "test_table/data/<process_date>/*/*.avro")),
        sqlQuery = "SELECT * from test_table t1 where t1.date = <process_date>"
      ),
      _processDate = "2000-01-01",
      _sourceBucket = "src-bkt",
      _outputBucket = "out-bkt"
    )
    val expected = EntityRequest(
      entityTable =  TablePartition(
        tableId = "test_entity",
        partitionUrl = "pe/pe_entities/test_entity/data/2000/01/01",
        hostBucket = "out-bkt"
      ),
      bqDataset = "pe_entities",
      processDate = LocalDate.of(2000, 1, 1),
      requiredTables = List(TableReference(
        tableId = "test_table",
        regexUrl = """^(test_table/data/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+)/[-._a-z0-9]+\.avro$""".r,
        stemUrl = "test_table/data",
        processDate = LocalDate.of(2000, 1, 1),
        hostBucket = "src-bkt",
        fileFormat = "avro",
        schemaSource = None,
        loadType = TableLoadType.INTERVAL,
        partitionsDays = 1,
        partitionDelay = 0
      )),
      "SELECT * from test_table t1 where t1.date = 2000-01-01",
      Nil
    )
    assertResult(expected)(actual)
  }
  it should "produce an EntityRequest with an empty errorList on valid non minimal InputEntityRequest" in {
    val actual = EntityRequestBuilder(
      inputRequestFile = InputEntityRequest(
        entityName = "test_entity",
        countryCode = "ar",
        requiredTables = List(InputTableSpec(
          tableId = "test_table",
          coreUrl = "test_table/data/<process_date>/*/*.tsv",
          schemaFile = Some("gs://my-bkt/schema/registry/file.json"),
          loadType = Some("MOST_RECENT"),
          hostBucket = Some("other-bkt"),
          partitionsDays = Some(3),
          partitionDelay = Some(1)
        )),
        sqlQuery = "SELECT * from test_table t1 where t1.date = <process_date>",
        bqDataset = Some("specific_dataset"),
        storageUrl = Some("specific/output/path")
      ),
      _processDate = "2000-01-01",
      _sourceBucket = "src-bkt",
      _outputBucket = "out-bkt"
    )
    val expected = EntityRequest(
      entityTable =  TablePartition(
        tableId = "test_entity",
        partitionUrl = "specific/output/path/2000/01/01",
        hostBucket = "out-bkt"
      ),
      bqDataset = "specific_dataset",
      processDate = LocalDate.of(2000, 1, 1),
      requiredTables = List(TableReference(
        tableId = "test_table",
        regexUrl = """^(test_table/data/\d{4}/\d{2}/\d{2}/[-._a-z0-9]+)/[-._a-z0-9]+\.tsv$""".r,
        stemUrl = "test_table/data",
        processDate = LocalDate.of(2000, 1, 1),
        hostBucket = "other-bkt",
        fileFormat = "tsv",
        schemaSource = Some("gs://my-bkt/schema/registry/file.json"),
        loadType = TableLoadType.MOST_RECENT,
        partitionsDays = 3,
        partitionDelay = 1
      )),
      "SELECT * from test_table t1 where t1.date = 2000-01-01",
      Nil
    )
    assertResult(expected)(actual)
  }
  it should "produce an EntityRequest with a non empty errorList on invalid InputEntityRequest" in {
    val actual = EntityRequestBuilder(
      inputRequestFile = InputEntityRequest(
        entityName = "test+entity",
        countryCode = "lo",
        requiredTables = List(null),
        sqlQuery = null,
        bqDataset = Some("invalid:dataset"),
        storageUrl = Some("gs://invalid/output/path")
      ),
      _processDate = "01-01-2000",
      _sourceBucket = "gs://src-bkt",
      _outputBucket = "gs://out-bkt"
    )
    val expected = EntityRequest(
      entityTable =  TablePartition(
        tableId = "_",
        partitionUrl = "none/none_entities/_/data/0001/01/01",
        hostBucket = "_",
      ),
      bqDataset = "none_entities",
      processDate = LocalDate.of(1, 1, 1),
      requiredTables = Nil,
      "_",
      List(
        "RequestException: invalid processDate [01-01-2000]",
        "RequestException: invalid sourceBucket [gs://src-bkt]",
        "RequestException: invalid outputBucket [gs://out-bkt]",
        "RequestException: invalid entityName [test+entity]",
        "RequestException: invalid countryCode [lo]",
        "RequestException: invalid requiredTables [List(null)]",
        "RequestException: invalid sqlQuery [null]",
        "RequestException: invalid storageUrl [gs://invalid/output/path]",
        "RequestException: invalid bqDataset [invalid:dataset]"
      )
    )
    assertResult(expected)(actual)
  }

}
