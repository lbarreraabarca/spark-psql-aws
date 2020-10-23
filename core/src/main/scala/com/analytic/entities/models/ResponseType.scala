package com.analytic.entities.models

object ResponseType extends Enumeration {
  type ResponseType = Value
  val SYSTEM_ERROR, ARGUMENTS_ERROR, RESPONSE_SUCCESS = Value
}
