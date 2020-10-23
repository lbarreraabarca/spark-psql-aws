package com.analytic.entities.adapters

import java.time.LocalDate
import java.time.Period

import com.analytic.entities.exceptions.BigQueryException
import com.analytic.entities.models.TableLoadType
import com.typesafe.scalalogging.Logger

import scala.util.{Failure, Success, Try}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, FormatOptions, QueryJobConfiguration, TimePartitioning}
import com.google.cloud.bigquery.{DatasetId, DatasetInfo, TableId}
import com.google.cloud.bigquery.{Job, JobId, JobInfo, LoadJobConfiguration}
import com.google.cloud.bigquery.Clustering
import TableLoadType.TableLoadType
import com.analytic.entities.ports.TBigQueryOperator

class BigQueryOperator extends TBigQueryOperator {
  private val log: Logger = Logger(classOf[BigQueryOperator])

  private def bigqueryService: BigQuery = BigQueryOptions.getDefaultInstance.getService

  def loadPartitionedTable(path: String, dataset: String, table: String, partitionDate: LocalDate, bqOptions: Map[String, String], format: String = "avro"): BigInt =
    if (path == null || path.trim.isEmpty) throw new BigQueryException("path cannot be null nor empty")
    else if (dataset == null || dataset.trim.isEmpty) throw new BigQueryException("dataset cannot be null nor empty")
    else if (table == null || table.trim.isEmpty) throw new BigQueryException("table cannot be null nor empty")
    else if (partitionDate == null) throw new BigQueryException("partitionDate cannot be null")
    else {
      val partitionedTableName = table + "$" + partitionDate.toString.replace("-", "")
      val tableId = TableId.of(dataset, partitionedTableName)
      val loadJobCnf = getDefaultLoadJobCnf(path, tableId, format, bqOptions)
      log.info(s"Creating BigQuery load job for $dataset.$partitionedTableName")
      val job: Job = Try(bigqueryService.create(JobInfo.of(JobId.of(java.util.UUID.randomUUID.toString), loadJobCnf))) match {
        case Success(j) => j
        case Failure(x) => throw new BigQueryException(
          s"Unable to create load job into table $dataset.$partitionedTableName - ${x.getClass}: ${x.getMessage}")
      }
      val completedJob = job.waitFor()
      if (completedJob == null) throw new BigQueryException("Job not executed since it no longer exists.")
      else if (completedJob.getStatus.getError != null)
        throw new BigQueryException(s"Error loading table to BigQuery: ${job.getStatus.getError}")
      else {
        val numRows = bigqueryService.getTable(tableId).getNumRows
        if (numRows.intValue() == 0)
          throw new BigQueryException(s"No rows were loaded to BigQuery possibly due to an empty Avro in URL $path")
        else {
          log.info(s"Successfully loaded $numRows rows into $dataset.$partitionedTableName.")
          numRows
        }
      }
    }

  def getAvailablePartitions(table: String, partitionCol: String, partitionDateMin: LocalDate, partitionDate: LocalDate, loadType: TableLoadType = TableLoadType.INTERVAL): (LocalDate, LocalDate) = {
    if (table == null || table.trim.isEmpty) throw new BigQueryException(s"Input table cannot be null nor empty")
    if (partitionCol == null || partitionCol.trim.isEmpty) throw new BigQueryException(s"Input partitionCol cannot be null nor empty")
    if (partitionDateMin == null) throw new BigQueryException(s"Input partitionDateMin cannot be null.")
    if (partitionDate == null) throw new BigQueryException(s"Input partitionDate cannot be null.")
    val startDate = "2015-01-01"
    val query = s"SELECT CAST(MIN(${partitionCol}) AS STRING) as min_partition, CAST(MAX(${partitionCol}) AS STRING) as max_partition  FROM ${table} WHERE ${partitionCol} BETWEEN '${startDate}' AND '${partitionDate.toString}'"

    val partitionList = try {
      val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query).build()
      bigqueryService.query(queryConfig).iterateAll()
    } catch {
      case _ => throw new BigQueryException(s" invalid query get partitions from bigQuery ${table}")
    }

    val partitionRange = partitionList.toString.replaceAll("[^0-9-,]", "").replace(",,",";").replace(",", "").split(";")
    if (partitionRange.size == 2) {
      val (minPartition, maxPartition): (LocalDate, LocalDate) = (LocalDate.parse(partitionRange(0)), LocalDate.parse(partitionRange(1)))
      loadType match {
        case TableLoadType.MOST_RECENT => (maxPartition.minusDays(1), maxPartition)
        case TableLoadType.TOTAL => (minPartition, maxPartition)
        case TableLoadType.ALL_DATA => (partitionDateMin, partitionDate)
        case TableLoadType.INTERVAL => (partitionDateMin, partitionDate)
        case TableLoadType.MOST_RECENT_UNTIL =>  {
          val period = Period.between(partitionDateMin, partitionDate)
          (maxPartition.minusDays(period.getDays), maxPartition)
        }
        case _ => throw new BigQueryException(s"Invalid load Type.")
      }
    } else throw new BigQueryException(s"No partitions available for ${table} (${startDate},${partitionDate.toString})")
  }

  def datasetExists(dataset: String): Boolean = bigqueryService.getDataset(DatasetId.of(dataset)) != null

  def createDataset(dataset: String): Unit = {
    log.info(s"Creating dataset $dataset in BigQuery.")
    val datasetInfo = DatasetInfo.newBuilder(dataset).build
    Try(bigqueryService.create(datasetInfo)) match {
      case Success(ds) => log.info(s"Dataset ${ds.getDatasetId.getDataset} created successfully in BigQuery")
      case Failure(x) => throw new BigQueryException(s"Unable to create dataset $dataset - ${x.getClass}: ${x.getMessage}")
    }
  }

  private def getDefaultLoadJobCnf(path: String, tableId: TableId, fileFormat: String, bqOptions: Map[String, String]): LoadJobConfiguration = {
    import scala.collection.JavaConverters._
    fileFormat match {
      case "avro" =>
        val loadJobConf = LoadJobConfiguration.newBuilder (tableId, path.stripSuffix ("/") + "/*.avro")
        .setFormatOptions (FormatOptions.avro)
        .setCreateDisposition (JobInfo.CreateDisposition.CREATE_IF_NEEDED)
        .setWriteDisposition (JobInfo.WriteDisposition.WRITE_TRUNCATE)
        .setTimePartitioning (
        TimePartitioning
        .newBuilder (TimePartitioning.Type.DAY)
        .setRequirePartitionFilter (true)
        .build)
        .setUseAvroLogicalTypes (true)
        if (bqOptions.contains("clusterBy"))
          loadJobConf.setClustering(Clustering
            .newBuilder()
            .setFields(bqOptions("clusterBy").split(",").toList.asJava)
            .build())
        loadJobConf.build()
     case _ => throw new BigQueryException (s"unimplemented file format [$fileFormat]")

    }
  }
}
