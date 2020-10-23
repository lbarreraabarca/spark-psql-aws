package com.analytic.entities.models

import java.time.LocalDate

case class EntityRequest(
  entityTable: TablePartition,
  bqDataset: String,
  processDate: LocalDate,
  requiredTables: List[TableReference],
  sqlQuery: String,
  errorList: List[String],
  bqOptions: Map[String, String] = Map.empty[String, String],
)
