package com.analytic.entities.adapters

import java.time.LocalDate

import com.analytic.entities.SharedTestSpark
import com.analytic.entities.exceptions.DataIOException
import com.analytic.entities.ports.DataIOOperator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.DataFrame

class SparkDataOperatorTest extends AnyFlatSpec with BeforeAndAfterAll {
  val testAvroOperator: DataIOOperator = new SparkDataOperator with SharedTestSpark with GoogleCloudStorageOperator
  val testAvro = "src/test/resources/my-other-bucket/cl/cl_entities/table_a/data/2020/03/21"
  val testTsv = "src/test/resources/tsv-bkt/path/data"
  val tsvSchema = "src/test/resources/tsv-bkt/path/schema/schema.json"
  var testDF: DataFrame = _
  var emptyDF: DataFrame = _

  override def beforeAll {
    testDF = testAvroOperator.readAvro(testAvro)
    emptyDF = testAvroOperator.readAvro("src/test/resources/emptydf/")
  }

  "readAvro" should "read test Avro file into a DataFrame and return the correct metrics." in {
    val df = testAvroOperator.readAvro(testAvro)
    val dfsCount: (Long, Int) =  (df.count, df.columns.length)
    assertResult((3, 2))(dfsCount)
  }
  it should "throw DataIOException if path is null." in {
    assertThrows[DataIOException] {
      testAvroOperator.readAvro(null)
    }
  }
  it should "throw DataIOException if path is empty." in {
    assertThrows[DataIOException] {
      testAvroOperator.readAvro("")
    }
  }
  it should "throw DataIOException if file is nonexistent." in {
    assertThrows[DataIOException] {
      testAvroOperator.readAvro("nonexistent_file")
    }
  }

  "readCsv" should "read .tsv files into a DataFrame and return the correct metrics" in {
    val df = testAvroOperator.readCsv(testTsv, tsvSchema, "\t")
    val dfsCount: (Long, Int) =  (df.count, df.columns.length)
    assertResult((5, 3))(dfsCount)
  }
  it should "throw DataIOException on invalid schema file." in {
    assertThrows[DataIOException] {
      testAvroOperator.readCsv(testTsv, "core_entity/src/test/resources/request_test.yaml", "\t")
    }
  }
  it should "throw DataIOException on nonexistent schema file." in {
    assertThrows[DataIOException] {
      testAvroOperator.readCsv(testTsv, "nonexistent-schema-file", "\t")
    }
  }
  it should "throw DataIOException if schemaUrl is null." in {
    assertThrows[DataIOException] {
      testAvroOperator.readCsv(testTsv, null, "\t")
    }
  }
  it should "throw DataIOException if schemaUrl is empty." in {
    assertThrows[DataIOException] {
      testAvroOperator.readCsv(testTsv, "", "\t")
    }
  }
  it should "throw DataIOException if path is null." in {
    assertThrows[DataIOException] {
      testAvroOperator.readCsv(null, tsvSchema, "\t")
    }
  }
  it should "throw DataIOException if path is empty." in {
    assertThrows[DataIOException] {
      testAvroOperator.readCsv("", tsvSchema, "\t")
    }
  }
  it should "throw DataIOException if file is inexistent." in {
    assertThrows[DataIOException] {
      testAvroOperator.readCsv("inexistent_file", tsvSchema, "\t")
    }
  }


  "readBigQuery" should "read bigQuery tables into a DataFrame and return the correct metrics" in {
    val dateRange = (LocalDate.of(2020, 3, 16), LocalDate.of(2020, 3,17))
    val df = testAvroOperator.readBigQuery("dbmark.lk_locales", "_PARTITIONDATE", dateRange)
    val dfsCount: (Long, Int) =  (df.count, df.columns.length)
    assertResult((964, 37))(dfsCount)
  }
  it should "throw DataIOException if table is not exist." in {
    assertThrows[DataIOException] {
      val dateRange = (LocalDate.of(2020, 3, 16), LocalDate.of(2020, 3,17))
        testAvroOperator.readBigQuery("fake_dataset.fake_table_test", "_PARTITIONDATE", dateRange)
    }
  }
  it should "throw DataIOException if table is empty." in {
    assertThrows[DataIOException] {
      val dateRange = (LocalDate.of(2020, 3, 16), LocalDate.of(2020, 3,17))
      testAvroOperator.readBigQuery("", "_PARTITIONDATE", dateRange)
    }
  }
  it should "read an empty DataFrame from BigQuery table." in {
    val dateRange = (LocalDate.of(2020, 3, 18), LocalDate.of(2020, 3,18))
    val df = testAvroOperator.readBigQuery("dbmark.lk_locales", "_PARTITIONDATE", dateRange)
    val dfsCount: (Long, Int) =  (df.count, df.columns.length)
    assertResult((0, 37))(dfsCount)
  }


  "saveAvro" should "return true if DataFrame was succesfully saved." in {
    assert(testAvroOperator.saveAvro("/tmp/test", testDF))
  }
  it should "throw DataIOException if DataFrame is empty." in {
    assertThrows[DataIOException] {
      testAvroOperator.saveAvro("/tmp/empty_df", emptyDF)
    }
  }
  it should "throw DataIOException if path is null." in {
    assertThrows[DataIOException] {
      testAvroOperator.saveAvro(null, testDF)
    }
  }
  it should "throw DataIOException if path is empty." in {
    assertThrows[DataIOException] {
      testAvroOperator.saveAvro("", testDF)
    }
  }
  it should "throw DataIOException if path is unreachable." in {
    assertThrows[DataIOException] {
      testAvroOperator.saveAvro("/", testDF)
    }
  }

}
