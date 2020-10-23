package com.analytic.entities.ports

import org.apache.spark.sql.DataFrame

trait SQLOperator {
  def executeQuery(query: String): DataFrame
  def createTempView(df: DataFrame)(viewName: String): Unit
}
