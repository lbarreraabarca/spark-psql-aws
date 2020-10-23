package com.analytic.entities.models

case class InputTableSpec(
  tableId: String,
  coreUrl: String,
  schemaFile: Option[String] = None,
  hostBucket: Option[String] = None,
  loadType: Option[String] = None,
  partitionsDays: Option[Int] = None,
  partitionDelay: Option[Int] = None,
)
