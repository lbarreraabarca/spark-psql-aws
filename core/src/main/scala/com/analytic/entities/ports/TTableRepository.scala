package com.analytic.entities.ports

import com.analytic.entities.models.{TablePartition, TableReference}

import scala.util.matching.Regex

trait TTableRepository {
  def generateRepository(targetTables: List[TableReference]): List[TablePartition]
  def getPartitionURL(repository: List[TablePartition])(targetTable: TableReference): String
  def matchParseURL(url: String, regexUrl: Regex, tableId: String, hostBucket: String): Option[TablePartition]
}
