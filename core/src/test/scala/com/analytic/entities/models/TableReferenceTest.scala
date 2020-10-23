package com.analytic.entities.models

import org.scalatest.flatspec.AnyFlatSpec
import java.time.LocalDate

class TableReferenceTest extends AnyFlatSpec {

  val tableRefA = TableReference(
    tableId = "test_table_a",
    regexUrl = """^(test_table/data/[-._a-z0-9]+/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.avro""".r,
    stemUrl = "test_table_a/data",
    processDate = LocalDate.of(2017, 12, 24),
    hostBucket = "my-src-bkt-01",
  )
  val tableRefB = TableReference(
    tableId = "test_table_b",
    regexUrl = """^(test_table/data/[-._a-z0-9]+/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.tsv""".r,
    stemUrl = "test_table_b/data",
    processDate = LocalDate.of(2017, 12, 24),
    hostBucket = "my-src-bkt-01",
    fileFormat = "tsv",
    schemaSource = None,
    loadType = TableLoadType.MOST_RECENT,
    partitionsDays = 2,
    partitionDelay = 3
  )

  "partitionDate" should "return same processDate if partitionDelay is 0" in {
    val actual = tableRefA.partitionDate
    val expected = java.time.LocalDate.of(2017,12,24)
    assertResult(expected)(actual)
  }
  it should "return processDate minus partitionDelay days" in {
    val actual = tableRefB.partitionDate
    val expected = java.time.LocalDate.of(2017,12,21)
    assertResult(expected)(actual)
  }

  "partitionDateMin" should "return partitionsDays before partitionDate" in {
    val actual = tableRefA.partitionDateMin
    val expected = java.time.LocalDate.of(2017,12,23)
    assertResult(expected)(actual)
  }
  it should "return partitionsDays before partitionDate when delayed" in {
    val actual = tableRefB.partitionDateMin
    val expected = java.time.LocalDate.of(2017,12,19)
    assertResult(expected)(actual)
  }

  "toString" should "return the correct representation of the table" in {
    val actual = tableRefB.toString
    val expected = "MOST_RECENT TABLE test_table_b @ gs://my-src-bkt-01/test_table_b/data [tsv] (2017-12-21 - 2017-12-19)"
    assertResult(expected)(actual)
  }

  "equals" should "return true for two similar TableSummary" in {
    val tableRef = TableReference(
      tableId = "test_table_b",
      regexUrl = """^(test_table/data/[-._a-z0-9]+/\d{4}/\d{2}/\d{2})/[-._a-z0-9]+\.tsv""".r,
      stemUrl = "test_table_b/data",
      processDate = LocalDate.of(2017, 12, 24),
      hostBucket = "my-src-bkt-01",
      fileFormat = "tsv",
      schemaSource = None,
      loadType = TableLoadType.MOST_RECENT,
      partitionsDays = 2,
      partitionDelay = 3
    )
    assert(tableRef == tableRefB)
  }
  it should "return false for two dissimilar TableSummary" in {
    assert(tableRefA != tableRefB)
  }
  it should "return false for two dissimilar class instances" in {
    assert(tableRefA != 4)
  }

}
