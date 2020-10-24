package com.analytic.entities.models

case class InputTableSpec(
  tableId: String,
  rdms: String,
  dbHost: String,
  dbPort: String,
  dbName: String,
  dbUsername: String,
  dbPassword: String,
  querySource: String,
)
