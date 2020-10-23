package com.analytic.entities.adapters

import com.analytic.entities.exceptions.YAMLException
import com.analytic.entities.models.{InputEntityRequest, InputTableSpec}
import org.scalatest.flatspec.AnyFlatSpec

class YAMLOperatorTest extends AnyFlatSpec {
  import io.circe.generic.auto._
  val yamlOperator = new YAMLOperator with GoogleCloudStorageOperator
  val testRequestPath = "src/test/resources/request_test.yaml"
  
  "parseFileTo" should "throw YAMLException if path is null." in {
    assertThrows[YAMLException] {
      yamlOperator.parseFileTo[Map[String, String]](null)
    }
  }
  it should "throw YAMLException if path is empty." in {
    assertThrows[YAMLException] {
      yamlOperator.parseFileTo[Map[String, String]]("")
    }
  }

  it should "throw YAMLException if destination case class is incompatible." in {
    assertThrows[YAMLException] {
      yamlOperator.parseFileTo[Map[String, Int]](testRequestPath)
    }
  }

  it should "successfully parse a YAML file content into the given case class" in {
    import com.analytic.entities.models.InputEntityRequest
    val actual = yamlOperator.parseFileTo[InputEntityRequest](testRequestPath)
    val expected = InputEntityRequest(
      entityName = "test_entity",
      countryCode = "ar",
      requiredTables = List(
        InputTableSpec(
          tableId = "cl_entities__table_a",
          coreUrl = "cl/cl_entities/table_a/data/<process_date>/*.avro",
          hostBucket = Some("my-other-bucket"),
          partitionDelay = Some(1)
        ),
        InputTableSpec(
          tableId = "schemay__table_b",
          coreUrl = "/orcl/pe/db/schemay/table_b/data/<process_date>/*.avro",
          partitionsDays = Some(180)
        ),
        InputTableSpec(
          tableId = "schemaz__table_c",
          coreUrl = "orcl/co/db/schemaz/table_c/data/<process_date>/*.avro",
          loadType = Some("ALL_DATA")
        ),
      ),
      sqlQuery =
        "SELECT ta.word, SUM(ta.number) suma " +
        "FROM cl_entities__table_a ta " +
        "JOIN schemay__table_b tb ON ta.number = tb.number " +
        "JOIN schemaz__table_c tc ON ta.number = tc.number " +
        "GROUP BY ta.word"
    )
    assertResult(expected)(actual)
  }

  "parseTo" should "throw exception on invalid input" in {
    assertThrows[YAMLException]{
      yamlOperator.parseTo[Map[String, String]](null)
    }
  }

}
