package com.analytic.entities.models

import java.time.LocalDate

import org.scalatest.flatspec.AnyFlatSpec

class TablePartitionTest extends AnyFlatSpec {

  val tablePart: TablePartition = TablePartition(
    tableId = "test_entity",
    partitionUrl = "ar/ar_entities/data/2017/12/24",
    hostBucket = "my-out-bkt-02"
  )

  "fullURL" should "return the correct URL" in {
    val actual = tablePart.fullUrl
    val expected = "gs://my-out-bkt-02/ar/ar_entities/data/2017/12/24"
    assertResult(expected)(actual)
  }

  "partitionDate" should "return the embedded date in url" in {
    val actual = tablePart.partitionDate
    val expected = LocalDate.of(2017,12,24)
    assertResult(expected)(actual)
  }
  it should "return the default date 0001-01-01 if no date is found" in {
    val tablePartUnique = TablePartition(
      tableId = "test_entity",
      partitionUrl = "ar/ar_entities/data",
      hostBucket = "my-out-bkt-02"
    )
    val actual = tablePartUnique.partitionDate
    val expected = LocalDate.of(1,1,1)
    assertResult(expected)(actual)
  }


}
