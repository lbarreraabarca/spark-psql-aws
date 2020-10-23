package com.analytic.entities.adapters

import java.time.LocalDate

import com.analytic.entities.exceptions.DataIOException
import com.analytic.entities.models.Spark
import com.analytic.entities.ports.{CloudStorageOperator, DataIOOperator}
import com.analytic.entities.utils.CoreUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{DataType, StructType}

class SparkDataOperator extends DataIOOperator { self: Spark with CloudStorageOperator =>
  private val log: Logger = Logger(classOf[SparkDataOperator])

  private def getSchema(schemaUrl: String): StructType = {
    val jsonContent: String = readFile(schemaUrl)
    DataType.fromJson(jsonContent).asInstanceOf[StructType]
  }

  def readCsv(paths: String, schemaUrl: String, sep: String): DataFrame =
    if (paths == null || paths.trim.isEmpty) throw new DataIOException(s"Input paths cannot be null nor empty")
    else if (schemaUrl == null || schemaUrl.trim.isEmpty) throw new DataIOException(s"schemaUrl cannot be null nor empty")
    else try {
      val inputPaths: Array[String] = paths.split(",")
      val schema: StructType = getSchema(schemaUrl)
      log.info(s"Reading Csv from ${CoreUtils.prettyPathFiles(paths)}")
      sparkSession.read.format("csv").option("sep", sep).schema(schema).load(inputPaths: _*)
    } catch {
      case x: Exception =>
        throw new DataIOException(s"Unable to read dataframe from paths $paths - ${x.getClass}: ${x.getMessage}")
    }

  def readAvro(paths: String): DataFrame =
    if (paths == null || paths.trim.isEmpty) throw new DataIOException(s"Input paths cannot be null nor empty")
    else try {
      log.info(s"Reading Avro from ${CoreUtils.prettyPathFiles(paths)}")
      val inputPaths: Array[String] = paths.split(",")
      sparkSession.read.format("avro").load(inputPaths: _*)
    } catch {
      case x: Exception =>
        throw new DataIOException(s"Unable to read dataframe from paths $paths - ${x.getClass}: ${x.getMessage}")
    }

  def readBigQuery(table: String, partitionCol: String, dateRange: (LocalDate, LocalDate)): DataFrame =
    if (table == null || table.trim.isEmpty) throw new DataIOException(s"Input table cannot be null nor empty")
    else try {
      log.info(s"Reading BigQuery table from $table with interval (${dateRange._1},${dateRange._2})")
      sparkSession.read.format("bigquery")
        .option("filter", s"$partitionCol > '${dateRange._1}' and $partitionCol <= '${dateRange._2}' ")
        .load(table)
    } catch {
      case x: Exception =>
        throw new DataIOException(s"Unable to read dataframe from table $table and partitionCol $partitionCol - ${x.getClass}: ${x.getMessage}")
    }

  def saveAvro(path: String, df: DataFrame): Boolean =
    if (df.isEmpty) throw new DataIOException("DataFrame to be written is empty")
    else if (path == null || path.trim.isEmpty) throw new DataIOException(s"Output path cannot be null nor empty")
    else try {
      log.info(s"Writing Avro to $path")
      df.write.mode(SaveMode.Overwrite).format("avro").save(path)
      true
    } catch {
      case x: Exception =>
        throw new DataIOException(s"Unable to write dataframe to path $path - ${x.getClass}: ${x.getMessage}")
    }

}
