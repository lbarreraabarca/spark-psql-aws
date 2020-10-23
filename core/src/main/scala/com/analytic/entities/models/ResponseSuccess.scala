package com.analytic.entities.models

class ResponseSuccess(_value: String = "") extends Response {

  val responseType: ResponseType.Value = ResponseType.RESPONSE_SUCCESS

  def isValid: Boolean = true
  def getType: String = responseType.toString
  def getValue: String = _value
 
}
