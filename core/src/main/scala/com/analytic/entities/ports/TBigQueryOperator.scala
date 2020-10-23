package com.analytic.entities.ports

import java.time.LocalDate

import com.analytic.entities.models.TableLoadType
import TableLoadType.TableLoadType

trait TBigQueryOperator {
  def loadPartitionedTable(path: String, dataset: String, table: String, partitionDate: LocalDate, bqOptions: Map[String, String], format: String = "avro"): BigInt
  def getAvailablePartitions(table: String, partitionCol: String, partitionDateMin: LocalDate, partitionDate: LocalDate, loadType: TableLoadType = TableLoadType.INTERVAL): (LocalDate, LocalDate)
  def datasetExists(datasetName: String): Boolean
  def createDataset(datasetName: String): Unit
}
