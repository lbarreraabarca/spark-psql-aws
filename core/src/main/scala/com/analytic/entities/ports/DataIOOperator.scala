package com.analytic.entities.ports

import java.time.LocalDate

import org.apache.spark.sql.DataFrame

trait DataIOOperator {
  def readPgSql(tableId: String, dbHost: String, dbPort: String, dbName: String, dbUsername: String, dbPassword: String, querySource: String): DataFrame
}
