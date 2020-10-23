package com.analytic.entities.models

class ResponseFailure(_value: String = "") extends Response {

  var responseType: ResponseType.Value = _

  def isValid: Boolean = false
  def getType: String = responseType.toString
  def getValue: String = _value

  def buildSystemError(value: String): ResponseFailure = {
    val response = new ResponseFailure(value)
    response.responseType = ResponseType.SYSTEM_ERROR
    response
  }

  def buildArgumentsError(value: String): ResponseFailure = {
    val response = new ResponseFailure(value)
    response.responseType = ResponseType.ARGUMENTS_ERROR
    response
  }


}
