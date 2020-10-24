package com.analytic.entities.models

object DataBaseType extends Enumeration {
  type DataBaseType = Value
  val POSTGRESQL, MYSQL, NONE = Value
}
