package com.analytic.entities.models


case class TableReference(
  tableId: String,
  rdms: DataBaseType.DataBaseType,
  dbHost: String,
  dbPort: String,
  dbName: String,
  dbUsername: String,
  dbPassword: String,
  querySource: String,
)
