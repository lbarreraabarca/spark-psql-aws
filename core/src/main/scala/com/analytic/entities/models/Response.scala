package com.analytic.entities.models

trait  Response {
  def isValid: Boolean
  def getType: String
  def getValue: String
}
