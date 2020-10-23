package com.analytic.entities.adapters

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}

import scala.util.Try
import java.time.LocalDate

import com.analytic.entities.exceptions.BigQueryException
import com.analytic.entities.models.TableLoadType
import com.google.cloud.bigquery.{BigQueryOptions, DatasetId}
import com.google.cloud.bigquery.{LoadJobConfiguration, TableId}
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption

class BigQueryOperatorTest extends AnyFlatSpec with PrivateMethodTester with BeforeAndAfterAll {
  val testBigQueryOperator = new BigQueryOperator
  val getDefaultLoadJobCnf: PrivateMethod[LoadJobConfiguration] = PrivateMethod[LoadJobConfiguration]('getDefaultLoadJobCnf)
  val testDataset = "test_generic_entity"
  val testDate = LocalDate.of(2020,1,1)
  val testAvroPath = "gs://datalake_retail_f0e87b52-b4b1-425b-a804-b40b09f11630/unit_tests_data/testdf"
  val testEmptyAvro = "gs://datalake_retail_f0e87b52-b4b1-425b-a804-b40b09f11630/unit_tests_data/emptydf"
  def deleteDataset(ds: String): Unit = {
    val bq = BigQueryOptions.getDefaultInstance.getService
    Try(bq.delete(DatasetId.of(ds), DatasetDeleteOption.deleteContents()))
  }

  "getLoadJobCnf" should "return LoadJobConfiguration with the given parameters" in {
    val outcome = testBigQueryOperator invokePrivate getDefaultLoadJobCnf(testAvroPath, TableId.of(testDataset, "testTable"), "avro", Map.empty[String, String])
    assert(outcome.isInstanceOf[LoadJobConfiguration])
    assert(outcome.getUseAvroLogicalTypes)
    assertResult("test_generic_entity")(outcome.getDestinationTable.getDataset)
    assertResult("testTable")(outcome.getDestinationTable.getTable)
    assertResult("AVRO")(outcome.getFormat)
    assertResult("WRITE_TRUNCATE")(outcome.getWriteDisposition.toString)
    assertResult(s"[$testAvroPath/*.avro]")(outcome.getSourceUris.toString)
    assertResult("TimePartitioning{type=DAY, expirationMs=null, field=null, requirePartitionFilter=true}"
    )(outcome.getTimePartitioning.toString)
  }
  it should "throw BigQueryException on invalid fileFormat" in {
    assertThrows[BigQueryException] {
      testBigQueryOperator invokePrivate getDefaultLoadJobCnf(testAvroPath, TableId.of("dataset", "table"), "xxx", Map.empty[String, String])
    }
  }
  it should "return LoadJobConfiguration with InputBigQueryOptions.clusterBy parameter." in {
    val outcome = testBigQueryOperator invokePrivate getDefaultLoadJobCnf(testAvroPath, TableId.of(testDataset, "testTable"), "avro", Map("clusterBy"-> "FIELD1"))
    assert(outcome.isInstanceOf[LoadJobConfiguration])
    assert(outcome.getUseAvroLogicalTypes)
    assertResult("test_generic_entity")(outcome.getDestinationTable.getDataset)
    assertResult("testTable")(outcome.getDestinationTable.getTable)
    assertResult("AVRO")(outcome.getFormat)
    assertResult("WRITE_TRUNCATE")(outcome.getWriteDisposition.toString)
    assertResult(s"[$testAvroPath/*.avro]")(outcome.getSourceUris.toString)
    assertResult("TimePartitioning{type=DAY, expirationMs=null, field=null, requirePartitionFilter=true}"
    )(outcome.getTimePartitioning.toString)
    assertResult("Clustering{fieldsImmut=[FIELD1]}")(outcome.getClustering.toString)
  }

  "createDataset" should "throw exception if unable to check/create dataset" in {
    val invalidDataset = "test.dataset"
    assertThrows[BigQueryException] {
      testBigQueryOperator.createDataset(invalidDataset)
    }
  }
  it should "throw exception if dataset already exists" in {
    val datasetName = "test_generic_entity"
    Try(testBigQueryOperator.createDataset(datasetName))
    assertThrows[BigQueryException] {
      testBigQueryOperator.createDataset(datasetName)
    }
    Try(deleteDataset(datasetName))
  }
  it should "create a dataset" in {
    val before = testBigQueryOperator.datasetExists(testDataset)
    assert(!before)
    testBigQueryOperator.createDataset(testDataset)
    val after = testBigQueryOperator.datasetExists(testDataset)
    assert(after)
    Try(deleteDataset(testDataset))
  }

  "datasetExists" should "return true if dataset already exists" in {
    testBigQueryOperator.createDataset(testDataset)
    val outcome = testBigQueryOperator.datasetExists(testDataset)
    assert(outcome)
    deleteDataset(testDataset)
  }
  it should "return false if dataset does not exist" in {
    val testDataset = "inexistent_dataset7654754"
    val outcome = testBigQueryOperator.datasetExists(testDataset)
    assert(!outcome)
  }

  "loadTableFromAvro" should "load avro to the given table.dataset and return its row count" in {
    Try(testBigQueryOperator.createDataset(testDataset))
    val rowCount = testBigQueryOperator.loadPartitionedTable(testAvroPath, testDataset, "testTable", testDate, Map.empty[String, String])
    assertResult(1)(rowCount)
    Try(deleteDataset(testDataset))
  }
  it should "throw BigQueryException whenever the number of rows uploaded is zero" in {
    Try(testBigQueryOperator.createDataset(testDataset))
    assertThrows[BigQueryException] {
      testBigQueryOperator.loadPartitionedTable(testEmptyAvro, testDataset, "testTable", testDate, Map.empty[String, String])
    }
    Try(deleteDataset(testDataset))
  }
  it should "throw BigQueryException on null dataset" in {
    assertThrows[BigQueryException] {
      testBigQueryOperator.loadPartitionedTable(testEmptyAvro, null, "testTable", testDate, Map.empty[String, String])
    }
  }
  it should "throw BigQueryException on null tableName" in {
    assertThrows[BigQueryException] {
      testBigQueryOperator.loadPartitionedTable(testEmptyAvro, testDataset, null, testDate, Map.empty[String, String])
    }
  }
  it should "throw BigQueryException on null path" in {
    assertThrows[BigQueryException] {
      testBigQueryOperator.loadPartitionedTable(null, testDataset, "testTable", testDate, Map.empty[String, String])
    }
  }
  it should "throw BigQueryException on null partitionDate" in {
    assertThrows[BigQueryException] {
      testBigQueryOperator.loadPartitionedTable(testEmptyAvro, testDataset, "testTable", null, Map.empty[String, String])
    }
  }
  it should "load avro to the given table.dataset with bqOptions and return its row count" in {
    Try(testBigQueryOperator.createDataset(testDataset))
    val rowCount = testBigQueryOperator.loadPartitionedTable(testAvroPath, testDataset, "testTable", testDate, Map("clusterBy"-> "numero"))
    assertResult(1)(rowCount)
    Try(deleteDataset(testDataset))
  }

  "getAvailablePartitions" should "Available partitions for MOST_RECENT load type." in {
    val actual = testBigQueryOperator.getAvailablePartitions("ar_entities.faar_prod_feed", "_partitiondate", LocalDate.of(2020, 9, 30), LocalDate.of(2020, 10, 1), TableLoadType.MOST_RECENT)
    val expected = (LocalDate.parse("2020-09-25"), LocalDate.parse("2020-09-26"))
    assertResult(expected)(actual)
  }
  it should "throw BigQueryException input table cannot be empty." in {
    val emptyTable = ""
    assertThrows[BigQueryException] {
      testBigQueryOperator.getAvailablePartitions(emptyTable, "_partitiondate", LocalDate.of(2019, 9, 30), LocalDate.of(2020, 10, 1), TableLoadType.INTERVAL)
    }
  }
  it should "throw BigQueryException partitionCol cannot be empty." in {
    val emptyPartitionCol = ""
    assertThrows[BigQueryException] {
      testBigQueryOperator.getAvailablePartitions("ar_entities.faar_prod_feed", emptyPartitionCol, LocalDate.of(2019, 9, 30), LocalDate.of(2020, 10, 1), TableLoadType.INTERVAL)
    }
  }
  it should "throw BigQueryException partitionDateMin cannot be empty." in {
    val emptyPartitionDateMin: LocalDate = null
    assertThrows[BigQueryException] {
      testBigQueryOperator.getAvailablePartitions("ar_entities.faar_prod_feed", "_partitiondate", emptyPartitionDateMin, LocalDate.of(2020, 10, 1), TableLoadType.INTERVAL)
    }
  }
  it should "throw BigQueryException partitionDate cannot be empty." in {
    val emptyPartitionDate: LocalDate = null
    assertThrows[BigQueryException] {
      testBigQueryOperator.getAvailablePartitions("ar_entities.faar_prod_feed", "_partitiondate", LocalDate.of(2020, 10, 1), emptyPartitionDate, TableLoadType.INTERVAL)
    }
  }
  it should "throw BigQueryException invalid query." in {
    val invalidTable = "ar_entities_fake_data.faar_prod_feed"
    assertThrows[BigQueryException] {
      testBigQueryOperator.getAvailablePartitions(invalidTable, "_partitiondate", LocalDate.parse("2020-10-01"), LocalDate.parse("2020-10-04"), TableLoadType.INTERVAL)
    }
  }
  it should "Available partitions for TOTAL load type." in {
    val actual = testBigQueryOperator.getAvailablePartitions("dbmark.lk_locales", "_partitiondate", LocalDate.parse("2020-09-30"), LocalDate.parse("2020-10-10"), TableLoadType.TOTAL)
    val expected = (LocalDate.parse("2019-08-08"), LocalDate.parse("2020-03-17"))
    assertResult(expected)(actual)
  }
  it should "Available partitions for ALL_DATA load type." in {
    val actual = testBigQueryOperator.getAvailablePartitions("dbmark.lk_locales", "_partitiondate", LocalDate.parse("2020-09-30"), LocalDate.parse("2020-10-10"), TableLoadType.ALL_DATA)
    val expected = (LocalDate.parse("2020-09-30"), LocalDate.parse("2020-10-10"))
    assertResult(expected)(actual)
  }
  it should "Available partitions for INTERVAL load type." in {
    val actual = testBigQueryOperator.getAvailablePartitions("dbmark.lk_locales", "_partitiondate", LocalDate.parse("2020-09-30"), LocalDate.parse("2020-10-10"), TableLoadType.INTERVAL)
    val expected = (LocalDate.parse("2020-09-30"), LocalDate.parse("2020-10-10"))
    assertResult(expected)(actual)
  }
  it should "Available partitions for MOST_RECENT_UNTIL load type." in {
    val actual = testBigQueryOperator.getAvailablePartitions("dbmark.lk_locales", "_partitiondate", LocalDate.parse("2020-10-09"), LocalDate.parse("2020-10-10"), TableLoadType.MOST_RECENT_UNTIL)
    val expected = (LocalDate.parse("2020-03-16"), LocalDate.parse("2020-03-17"))
    assertResult(expected)(actual)
  }
  it should "Available partitions for MOST_RECENT_UNTIL 3 days load type." in {
    val actual = testBigQueryOperator.getAvailablePartitions("dbmark.lk_locales", "_partitiondate", LocalDate.parse("2020-10-07"), LocalDate.parse("2020-10-10"), TableLoadType.MOST_RECENT_UNTIL)
    val expected = (LocalDate.parse("2020-03-14"), LocalDate.parse("2020-03-17"))
    assertResult(expected)(actual)
  }
  it should "throw BigQueryException invalid load type." in {
    val invalidLoadType: TableLoadType.TableLoadType = null
    assertThrows[BigQueryException] {
      testBigQueryOperator.getAvailablePartitions("dbmark.lk_locales", "_partitiondate", LocalDate.parse("2020-10-01"), LocalDate.parse("2020-10-04"), invalidLoadType)
    }
  }
}