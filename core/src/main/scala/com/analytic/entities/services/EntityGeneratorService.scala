package com.analytic.entities.services

import java.time.LocalDate

import com.analytic.entities.models.{EntityRequest, Response, ResponseFailure, ResponseSuccess, TableReference, DataBaseType}
import com.analytic.entities.ports.{DataIOOperator, SQLOperator}
import org.apache.spark.sql.DataFrame

class EntityGeneratorService(
  dataOperator: DataIOOperator,
  sparkOperator: SQLOperator) {

  if (dataOperator == null) throw new IllegalArgumentException("Avro operator cannot be null")
  if (sparkOperator == null) throw new IllegalArgumentException("Spark operator cannot be null")

  private def readTableData(tableRef: TableReference): DataFrame = try tableRef.rdms match {
    case DataBaseType.POSTGRESQL =>dataOperator.readPgSql(tableRef.tableId, tableRef.dbHost, tableRef.dbPort, tableRef.dbName, tableRef.dbUsername, tableRef.dbPassword, tableRef.querySource)
    case _ => throw new IllegalArgumentException("unimplemented database engine")
  } catch {
    case x: Exception => throw new RuntimeException(s"Unable to read table $tableRef. ${x.getClass}: ${x.getMessage}")
  }

  def invoke(request: EntityRequest): Response = {
    if (request.errorList.nonEmpty) {
      val errors: String = request.errorList.toString
      val responseHandler = new ResponseFailure
      val response = responseHandler.buildArgumentsError(errors)
      response
    } else try {
      val requiredTables: List[TableReference] = request.requiredTables
      val tablesDF: Seq[DataFrame] = requiredTables.map(readTableData(_))
      (tablesDF zip requiredTables).foreach {
        case (df: DataFrame, ts: TableReference) => sparkOperator.createTempView(df)(ts.tableId)
      }
      val entityDF: DataFrame = sparkOperator.executeQuery(request.sqlQuery)
      //TODO: WRITE PGSQL
      //TODO: PARQUET IN AWS S3
      val response = new ResponseSuccess("Its ok!")
      response
    } catch {
      case x: Exception =>
        val responseHandler = new ResponseFailure
        val response = responseHandler.buildSystemError(s"${x.getClass}: ${x.getMessage}")
        response
    }
  }

}
