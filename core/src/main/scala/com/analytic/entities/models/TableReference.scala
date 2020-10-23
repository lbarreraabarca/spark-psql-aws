package com.analytic.entities.models

import java.time.LocalDate
import scala.util.matching.Regex
import TableLoadType.TableLoadType

case class TableReference(
  tableId: String,
  regexUrl: Regex,
  stemUrl: String,
  processDate: LocalDate,
  hostBucket: String,
  fileFormat: String = "avro",
  schemaSource: Option[String] = None,
  loadType: TableLoadType = TableLoadType.INTERVAL,
  partitionsDays: Int = 1,
  partitionDelay: Int = 0
) {
  val partitionDate: LocalDate = processDate.minusDays(partitionDelay.toLong)
  val partitionDateMin: LocalDate = partitionDate.minusDays(partitionsDays.toLong)

  override def toString: String =
    s"$loadType TABLE $tableId @ gs://$hostBucket/$stemUrl [$fileFormat] ($partitionDate - $partitionDateMin)"
  override def equals(obj: Any): Boolean = obj match {
    case t: TableReference => t.regexUrl.toString == regexUrl.toString && t.toString == toString
    case _ => false
  }

}
