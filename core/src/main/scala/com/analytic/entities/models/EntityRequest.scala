package com.analytic.entities.models

import java.time.LocalDate
import CountryCode.CountryCode

case class EntityRequest(
  entityName: String,
  countryCode: CountryCode,
  processDate: LocalDate,
  requiredTables: List[TableReference],
  sqlQuery: String,
  errorList: List[String],
  //TODO: WRITE PGSQL
  //TODO: WRITE AWS
)
