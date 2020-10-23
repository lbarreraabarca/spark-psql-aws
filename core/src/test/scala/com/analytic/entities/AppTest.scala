package com.analytic.entities

import com.analytic.entities.exceptions.ControllerException
import com.analytic.entities.models.{InputEntityRequest, InputTableSpec, ResponseFailure}
import org.scalatest.flatspec.AnyFlatSpec

class AppTest extends AnyFlatSpec with SharedTestSpark {
  val testRequestPath = "src/test/resources/request_test.yaml"

  "readRequestFileCore" should "throw ControllerException if there is a problem with the path." in {
    assertThrows[ControllerException] {
      App.readRequestFile(null)
    }
  }
  it should "read the given path and return its content." in {
    val output = App.readRequestFile(testRequestPath)
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
    assertResult(expected)(output)
  }

  "makeRequestCore" should "make a valid request." in {
    val inputRequestFile = App.readRequestFile(testRequestPath)
    val output = App.makeRequestCore(
      inputRequestFile,
      "2020-04-01",
      "my-src-bucket",
      "my-out-bucket",
    )
    assert(output.errorList.isEmpty)
  }
  it should "throw ControllerException if there is a problem creating the request." in {
    assertThrows[ControllerException] {
      App.makeRequestCore(null, null, null, null)
    }
  }

  "validateResponse" should "throw ControllerException if response is not valid." in {
    val responseHandler = new ResponseFailure
    val response = responseHandler.buildArgumentsError("errs")
    assertThrows[ControllerException] {
      App.validateResponse(response)
    }
  }

  "main" should "throw exception if invalid arguments are given." in {
    assertThrows[ControllerException] {
      App.main(Array("this", "is", "invalid", "input"))
    }
  }
  it should "throw exception if there are missing arguments." in {
    assertThrows[ControllerException] {
      App.main(Array("invalid", "input"))
    }
  }

}
