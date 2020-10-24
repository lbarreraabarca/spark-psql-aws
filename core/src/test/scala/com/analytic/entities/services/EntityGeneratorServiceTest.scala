package com.analytic.entities.services

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.PrivateMethodTester
import org.scalamock.scalatest.MockFactory
import java.time.LocalDate

import com.analytic.entities.SharedTestSpark
import com.analytic.entities.adapters.{GoogleCloudStorageOperator, SparkDataOperator, SparkSQLOperator}
import com.analytic.entities.exceptions.RepositoryException
import com.analytic.entities.models.{EntityRequest, EntityRequestBuilder, InputEntityRequest, InputTableSpec, ResponseFailure, ResponseSuccess, ResponseType, TableLoadType, TableReference}
import com.analytic.entities.ports.{DataIOOperator, SQLOperator}
import org.apache.spark.sql.DataFrame

class EntityGeneratorServiceTest extends AnyFlatSpec with MockFactory with PrivateMethodTester with SharedTestSpark {
  val testService = new EntityGeneratorService(
    new SparkDataOperator with SharedTestSpark with GoogleCloudStorageOperator,
    new SparkSQLOperator with SharedTestSpark,
    new BigQueryOperator,
    new TableRepository with GoogleCloudStorageOperator
  )
  val readTableData = PrivateMethod[DataFrame]('readTableData)

  val testTablesRepository: List[TablePartition] = List(
    TablePartition(
      tableId = "cl_entities__table_a",
      partitionUrl = "cl/cl_entities/table_a/data/2020/03/21",
      hostBucket = "my-other-bucket",
      bktPrefix = "src/test/resources/"
    ),
    TablePartition(
      tableId = "cl_entities__table_a",
      partitionUrl = "cl/cl_entities/table_a/data/2020/05/01",
      hostBucket = "my-other-bucket",
      bktPrefix = "src/test/resources/"
    ),
    TablePartition(
      tableId = "tsv_data",
      partitionUrl = "path/data",
      hostBucket = "tsv-bkt",
      bktPrefix = "src/test/resources/"
    )
  )

  "readTableData" should "return a DataFrame with the respective avro file" in {
    val testTableRef = TableReference(
      tableId = "cl_entities__table_a",
      regexUrl = """^(cl/cl_entities/table_a/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro$""".r,
      stemUrl = "cl/cl_entities/table_a/data",
      hostBucket = "my-other-bucket",
      processDate = LocalDate.of(2020,4,1),
      partitionDelay = 11,
    )
    val actual = testService invokePrivate readTableData(testTablesRepository, testTableRef)
    val actualCount = actual.count()
    val actualColumns = actual.columns
    val expectedCount = 3
    val expectedColumns = Array("word", "number")

    assertResult(expectedCount)(actualCount)
    assertResult(expectedColumns.toSet)(actualColumns.toSet)
  }
  it should "return a DataFrame with the respective tsv file" in  {
    val testTableRef = TableReference(
      tableId = "tsv_data",
      regexUrl = """^(path/data)/[-._a-z0-9]+\.tsv$""".r,
      stemUrl = "path/data",
      fileFormat = "tsv",
      schemaSource = Some("src/test/resources/tsv-bkt/path/schema/schema.json"),
      hostBucket = "tsv-bkt",
      processDate = LocalDate.of(2020,4,1),
      loadType = TableLoadType.MOST_RECENT,
      partitionDelay = 11,
    )
    val actual = testService invokePrivate readTableData(testTablesRepository, testTableRef)
    val actualCount = actual.count()
    val actualColumns = actual.columns
    val expectedCount = 5
    val expectedColumns = Array("id", "value", "any")

    assertResult(expectedCount)(actualCount)
    assertResult(expectedColumns.toSet)(actualColumns.toSet)
  }
  it should "return a DataFrame with the respective bigquery table" in  {
    val testTableRef = TableReference(
      tableId = "bq_data",
      regexUrl = """^(dbmark\.lk_locales)/[-._a-z0-9]+\.bigquery$""".r,
      stemUrl = "dbmark.lk_locales$_PARTITIONDATE",
      fileFormat = "bigquery",
      hostBucket = "_",
      processDate = LocalDate.of(2020,3,17),
    )

    val actual = testService invokePrivate readTableData(Nil, testTableRef)
    val actualCount = actual.count
    val actualColumns = actual.columns.length
    val expectedCount = 964
    val expectedColumns = 37

    assertResult(expectedCount)(actualCount)
    assertResult(expectedColumns)(actualColumns)
  }
  it should "throw RuntimeException on nonexistent table" in {
    val testTableRef = TableReference(
      tableId = "nonexistent",
      regexUrl = """^(non/existent)/[-._a-z0-9]+\.avro$""".r,
      stemUrl = "non/existent",
      hostBucket = "my-other-bucket",
      processDate = LocalDate.of(2020,4,1),
    )
    assertThrows[RuntimeException] {
      testService invokePrivate readTableData(testTablesRepository, testTableRef)
    }
  }
  it should "throw RuntimeException on invalid table file format" in {
    val testTableRef = TableReference(
      tableId = "cl_entities__table_a",
      regexUrl = """^(cl/cl_entities/table_a/data/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.xml$""".r,
      stemUrl = "cl/cl_entities/table_a/data",
      hostBucket = "my-other-bucket",
      processDate = LocalDate.of(2020,4,1),
      partitionDelay = 11,
      fileFormat = "xml"
    )
    assertThrows[RuntimeException] {
      testService invokePrivate readTableData(testTablesRepository, testTableRef)
    }
  }

  "Constructor" should "raise IllegalArgumentException on null DataIOOperator." in {
    val dataOperator: DataIOOperator = null
    val sqlOperator = mock[SQLOperator]
    val bigQueryOperator = mock[TBigQueryOperator]
    val tableSummaryRepository = mock[TTableRepository]

    assertThrows[IllegalArgumentException] {
      new EntityGeneratorService(
        dataOperator,
        sqlOperator,
        bigQueryOperator,
        tableSummaryRepository)
    }
  }

  it should "raise IllegalArgumentException on null SparkOperator." in {
    val dataOperator = mock[DataIOOperator]
    val sqlOperator: SQLOperator = null
    val bigQueryOperator = mock[TBigQueryOperator]
    val tableSummaryRepository = mock[TTableRepository]

    assertThrows[IllegalArgumentException] {
      new EntityGeneratorService(
        dataOperator,
        sqlOperator,
        bigQueryOperator,
        tableSummaryRepository)
    }
  }

  it should "raise IllegalArgumentException on null TTableSummaryRepository." in {
    val dataOperator = mock[DataIOOperator]
    val sqlOperator = mock[SQLOperator]
    val bigQueryOperator = mock[TBigQueryOperator]
    val tableSummaryRepository = null

    assertThrows[IllegalArgumentException] {
      new EntityGeneratorService(
        dataOperator,
        sqlOperator,
        bigQueryOperator,
        tableSummaryRepository
      )
    }
  }

  it should "raise IllegalArgumentException on null TBigQueryOperator." in {
    val dataOperator = mock[DataIOOperator]
    val sqlOperator = mock[SQLOperator]
    val bigQueryOperator = null
    val tableSummaryRepository = mock[TTableRepository]

    //Assert
    assertThrows[IllegalArgumentException] {
      new EntityGeneratorService(
        dataOperator,
        sqlOperator,
        bigQueryOperator,
        tableSummaryRepository
      )
    }
  }

  it should "succeed on null TEncryptionCorrelatorOperator." in {
    val dataOperator = mock[DataIOOperator]
    val sqlOperator = mock[SQLOperator]
    val bigQueryOperator = mock[TBigQueryOperator]
    val tableSummaryRepository = mock[TTableRepository]

    val service = new EntityGeneratorService(
      dataOperator,
      sqlOperator,
      bigQueryOperator,
      tableSummaryRepository
    )
    assert(service.isInstanceOf[EntityGeneratorService])
  }

  "invoke" should "returns response failure arguments errors on invalid request." in {
    val inputRequestFile = InputEntityRequest(
      "test_entity",
      "cl",
      List(
        InputTableSpec(
          tableId = "cl_entities__table_a",
          coreUrl = "cl/cl_entities/table_a/data/*.avro",
          hostBucket = Some("my-other-bucket"),
          partitionDelay = Some(11),
        )
      ),
      "ERROR PRONE QUERY"
    )
    val request: EntityRequest = EntityRequestBuilder(
      inputRequestFile,
      "2020-12-08",
      "bkt-in",
      "bkt-out"
    )
    val dataOperator = mock[DataIOOperator]
    val sqlOperator = mock[SQLOperator]
    val bigQueryOperator = mock[TBigQueryOperator]
    val tableSummaryRepository = mock[TTableRepository]

    val service = new EntityGeneratorService(
      dataOperator,
      sqlOperator,
      bigQueryOperator,
      tableSummaryRepository
    )

    val response = service.invoke(request)

    assert(response.isInstanceOf[ResponseFailure])
    assertResult(ResponseType.ARGUMENTS_ERROR.toString)(response.getType)
  }

  it should "return response failure on adapter exception." in {
    val inputRequestFile = InputEntityRequest(
      "test_entity",
      "cl",
      List(
        InputTableSpec(
          tableId = "cl_entities__table_a",
          coreUrl = "cl/cl_entities/table_a/data/*.avro",
          hostBucket = Some("my-other-bucket"),
          partitionDelay = Some(11),
        )
      ),
      "SELECT * from cl_entities__table_a"
    )
    val request: EntityRequest = EntityRequestBuilder(
      inputRequestFile,
      "2020-12-08",
      "bkt-in",
      "bkt-out"
    )
    val dataOperator = mock[DataIOOperator]
    val sqlOperator = mock[SQLOperator]
    val bigQueryOperator = mock[TBigQueryOperator]
    val tableSummaryRepository = mock[TTableRepository]

    tableSummaryRepository.generateRepository _ expects * throws new RepositoryException("Adapter failed")

    val service = new EntityGeneratorService(
      dataOperator,
      sqlOperator,
      bigQueryOperator,
      tableSummaryRepository
    )

    val response = service.invoke(request)

    assert(response.isInstanceOf[ResponseFailure])
    assertResult(ResponseType.SYSTEM_ERROR.toString)(response.getType)
  }

  it should "return response success on valid input." in {
    //Arrange
    val testTablesRepository: List[TablePartition] = List(
      TablePartition(
        tableId = "cl_entities__table_a",
        partitionUrl = "cl/cl_entities/table_a/data/2020/04/01",
        hostBucket = "my-other-bucket",
        bktPrefix = "src/test/resources/"
      ),
      TablePartition(
        tableId = "cl_entities__table_a",
        partitionUrl = "cl/cl_entities/table_a/data/2020/05/01",
        hostBucket = "my-other-bucket",
        bktPrefix = "src/test/resources/"
      )
    )

    val inputRequestFile = InputEntityRequest(
      "test_entity",
      "cl",
      List(
        InputTableSpec(
          tableId = "cl_entities__table_a",
          coreUrl = "cl/cl_entities/table_a/data/<process_date>/*.avro",
          hostBucket = Some("my-other-bucket"),
          partitionDelay = Some(11),
        )
      ),
      "SELECT * FROM cl_entities__table_a"
    )
    val request: EntityRequest = EntityRequestBuilder(
      inputRequestFile,
      "2020-04-12",
      "in-bkt",
      "out-bkt"
    )

    val testURL = "src/test/resources/my-other-bucket/cl/cl_entities/table_a/data/2020/03/21"
    val df = sparkSession.read.format("avro").load(testURL)
    val testTablesDF = List(df)
    val testEntityDF = df

    val dataOperator = mock[DataIOOperator]
    val sqlOperator = mock[SQLOperator]
    val bigQueryOperator = mock[TBigQueryOperator]
    val tableSummaryRepository = mock[TTableRepository]

    //STEPS
    val targetTables: List[TableReference] = request.requiredTables
    val entityTable: TablePartition = request.entityTable

    tableSummaryRepository.generateRepository _ expects targetTables returns testTablesRepository
    (tableSummaryRepository.getPartitionURL (_: List[TablePartition])(_: TableReference)) expects(
      testTablesRepository,
      targetTables.head) returns testURL
    dataOperator.readAvro _ expects testURL returns testTablesDF.head
    (sqlOperator.createTempView (_: DataFrame)(_: String)) expects (testTablesDF.head, targetTables.head.tableId)
    sqlOperator.executeQuery _ expects request.sqlQuery returns testEntityDF
    dataOperator.saveAvro _ expects(entityTable.fullUrl, testEntityDF)
    bigQueryOperator.datasetExists _ expects request.bqDataset returns false
    bigQueryOperator.createDataset _ expects request.bqDataset
    bigQueryOperator.loadPartitionedTable _ expects(
      entityTable.fullUrl,
      request.bqDataset,
      entityTable.tableId,
      request.processDate,
      request.bqOptions,
      "avro"
    )

    val service = new EntityGeneratorService(
      dataOperator,
      sqlOperator,
      bigQueryOperator,
      tableSummaryRepository
    )
    //Act
    val response = service.invoke(request)

    //Assert
    assert(response.isInstanceOf[ResponseSuccess])
    assertResult(ResponseType.RESPONSE_SUCCESS.toString)(response.getType)
  }

}
