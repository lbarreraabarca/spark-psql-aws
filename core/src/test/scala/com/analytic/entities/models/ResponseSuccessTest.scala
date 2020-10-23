package com.analytic.entities.models

import org.scalatest.flatspec.AnyFlatSpec

class ResponseSuccessTest extends AnyFlatSpec {
  val response = new ResponseSuccess("test!")
  "isValid" should "return true." in {
    assert(response.isValid)
  }

  "getType" should "return ResponseType.RESPONSE_SUCCESS." in {
    assertResult(ResponseType.RESPONSE_SUCCESS.toString)(response.getType)
  }

  "getValue" should "return given string." in {
    assertResult("test!")(response.getValue)
  } 
  
}
