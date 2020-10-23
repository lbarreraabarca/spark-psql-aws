package com.analytic.entities.models

import scala.util.Try
import java.time.LocalDate
import com.analytic.entities.utils.CoreUtils.extractDate

case class TablePartition(tableId: String, partitionUrl: String, hostBucket: String, var bktPrefix: String = "gs://") {
  val partitionDate: LocalDate = Try(extractDate(partitionUrl)) getOrElse LocalDate.of(1,1,1)
  val fullUrl: String = bktPrefix + s"$hostBucket/$partitionUrl"
}
