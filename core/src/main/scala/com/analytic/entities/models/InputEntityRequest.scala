package com.analytic.entities.models

case class InputEntityRequest(
  entityName: String,
  countryCode: String,
  requiredTables: List[InputTableSpec],
  sqlQuery: String,
)
