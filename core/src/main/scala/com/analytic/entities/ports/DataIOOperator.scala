package com.analytic.entities.ports

import java.time.LocalDate

import org.apache.spark.sql.DataFrame

trait DataIOOperator {
  def readCsv(paths: String, schemaUrl: String, sep: String): DataFrame
  def readAvro(paths: String): DataFrame
  def readBigQuery(table: String, partitionCol: String, dateRange: (LocalDate, LocalDate)): DataFrame
  def saveAvro(path: String, df: DataFrame): Boolean
}
