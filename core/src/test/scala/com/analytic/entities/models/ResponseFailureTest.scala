package com.analytic.entities.models

import org.scalatest.flatspec.AnyFlatSpec

class ResponseFailureTest extends AnyFlatSpec {

  val response = new ResponseFailure("test!")

  "isValid" should "return false." in {
    assert(!response.isValid)
  }
  
  "getValue" should "return given string." in {
    assertResult("test!")(response.getValue)
  }

  "getType" should "return ARGUMENTS_ERRORS when buildArgumentsError is the constructor." in {
    val responseHandler = new ResponseFailure
    val responseFail = responseHandler.buildArgumentsError("argserr")

    assertResult(ResponseType.ARGUMENTS_ERROR.toString)(responseFail.getType)
  }

  "getType" should "return SYSTEM_ERROR when buildSystemError is the constructor." in {
    val responseHandler = new ResponseFailure
    val responseFail = responseHandler.buildSystemError("syserr")

    assertResult(ResponseType.SYSTEM_ERROR.toString)(responseFail.getType)
  }

}
