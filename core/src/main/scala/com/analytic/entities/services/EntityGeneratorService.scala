package com.analytic.entities.services

import java.time.LocalDate

import com.analytic.entities.models.{EntityRequest, Response, ResponseFailure, ResponseSuccess, TablePartition, TableReference}
import com.analytic.entities.ports.{DataIOOperator, SQLOperator, TBigQueryOperator, TTableRepository}
import org.apache.spark.sql.DataFrame

class EntityGeneratorService(
  dataOperator: DataIOOperator,
  sparkOperator: SQLOperator,
  bigQueryOperator: TBigQueryOperator,
  repositoryOperator: TTableRepository) {

  if (dataOperator == null) throw new IllegalArgumentException("Avro operator cannot be null")
  if (sparkOperator == null) throw new IllegalArgumentException("Spark operator cannot be null")
  if (bigQueryOperator == null) throw new IllegalArgumentException("BigQuery operator cannot be null")
  if (repositoryOperator == null) throw new IllegalArgumentException("TableSummary repository cannot be null")

  private def readTableData(repository: List[TablePartition])(tableRef: TableReference): DataFrame = try tableRef match {
    case t if t.fileFormat == "avro" =>
      dataOperator.readAvro(repositoryOperator.getPartitionURL(repository)(t))
    case t if t.fileFormat == "tsv"  =>
      dataOperator.readCsv(repositoryOperator.getPartitionURL(repository)(t), t.schemaSource.get, "\t")
    case t if t.fileFormat == "bigquery"  => {
      val (partitionMin, partitionMax): (LocalDate, LocalDate)= bigQueryOperator.getAvailablePartitions(t.stemUrl.split("\\$").head, t.stemUrl.split("\\$").last, t.partitionDateMin, t.partitionDate, t.loadType)
      dataOperator.readBigQuery(t.stemUrl.split("\\$").head, t.stemUrl.split("\\$").last, (partitionMin, partitionMax))
    }
    case _ => throw new IllegalArgumentException("unimplemented file format")
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
      val entityTable: TablePartition = request.entityTable
      val repository: List[TablePartition] = repositoryOperator.generateRepository(requiredTables.filter(_.fileFormat!="bigquery"))
      val tablesDF: Seq[DataFrame] = requiredTables.map(readTableData(repository)(_))
      (tablesDF zip requiredTables).foreach {
        case (df: DataFrame, ts: TableReference) => sparkOperator.createTempView(df)(ts.tableId)
      }
      val entityDF: DataFrame = sparkOperator.executeQuery(request.sqlQuery)
      dataOperator.saveAvro(entityTable.fullUrl, entityDF)
      if (request.bqDataset != "_") {
        if (!bigQueryOperator.datasetExists(request.bqDataset)) bigQueryOperator.createDataset(request.bqDataset)
        bigQueryOperator.loadPartitionedTable(path = entityTable.fullUrl,
          dataset = request.bqDataset,
          table = entityTable.tableId,
          partitionDate = request.processDate,
          bqOptions = request.bqOptions)

      }
      val response = new ResponseSuccess(entityTable.fullUrl)
      response
    } catch {
      case x: Exception =>
        val responseHandler = new ResponseFailure
        val response = responseHandler.buildSystemError(s"${x.getClass}: ${x.getMessage}")
        response
    }
  }

}
