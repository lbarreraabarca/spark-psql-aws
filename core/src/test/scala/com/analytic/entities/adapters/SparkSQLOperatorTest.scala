package com.analytic.entities.adapters

import com.analytic.entities.SharedTestSpark
import com.analytic.entities.exceptions.SparkException
import com.analytic.entities.ports.SQLOperator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.DataFrame

class SparkSQLOperatorTest extends AnyFlatSpec with SharedTestSpark with BeforeAndAfterAll {
  var testSparkOperator: SQLOperator = new SparkSQLOperator with SharedTestSpark
  var emptyDF: DataFrame = _
  var testDF: DataFrame = _

  override def beforeAll {
    emptyDF = loadAvro("src/test/resources/emptydf/")
    testDF = loadAvro("src/test/resources/my-other-bucket/cl/cl_entities/table_a/data/2020/03/21")
  }

  "createTempView" should "bring into existence a table in catalog " +
    "and with the correct name." in {
    val tableName: String = "test_table"
    val tableExistenceBefore = sparkSession.catalog.tableExists(tableName)
    assert(!tableExistenceBefore)
    testSparkOperator.createTempView(testDF)(tableName)
    val tableExistenceAfter: Boolean = sparkSession.catalog.tableExists(tableName)
    assert(tableExistenceAfter)
  }
  it should "throw SparkException if viewName is invalid" in {
    assertThrows[SparkException] {
      testSparkOperator.createTempView(testDF)("invalid.id")
    }
  }
  it should "throw SparkException if viewName is null" in {
    assertThrows[SparkException] {
      testSparkOperator.createTempView(testDF)(null)
    }
  }
  it should "throw SparkException if viewName is empty" in {
    assertThrows[SparkException] {
      testSparkOperator.createTempView(testDF)("")
    }
  }

  "executeSQL" should "throw SparkException if query is null." in {
    assertThrows[SparkException] {
      testSparkOperator.executeQuery(null)
    }
  }
  it should "throw SparkException if query is empty." in {
    assertThrows[SparkException] {
      testSparkOperator.executeQuery("")
    }
  }
  it should "throw SparkException if query is invalid." in {
    assertThrows[SparkException] {
      testSparkOperator.executeQuery("INVALID QUERY")
    }
  }
  it should "throw SparkException on valid query but invalid table." in {
    assertThrows[SparkException] {
      testSparkOperator.executeQuery("Select * from nonexistent_table")
    }
  }

}
