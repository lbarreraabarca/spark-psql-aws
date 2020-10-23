package com.analytic.entities.models

case class InputEntityRequest(
  entityName: String,
  countryCode: String,
  requiredTables: List[InputTableSpec],
  sqlQuery: String,
  bqDataset: Option[String] = None,
  storageUrl: Option[String] = None,
  bqOptions: Option[Map[String, String]] = None,
)
