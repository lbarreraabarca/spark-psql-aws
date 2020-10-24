package com.analytic.entities.adapters

import com.analytic.entities.exceptions.DataIOException
import com.analytic.entities.models.Spark
import com.analytic.entities.ports.{CloudStorageOperator, DataIOOperator}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.util.Properties

class SparkDataOperator extends DataIOOperator { self: Spark with CloudStorageOperator =>
  private val log: Logger = Logger(classOf[SparkDataOperator])

  def readPgSql(tableId: String, dbHost: String, dbPort: String, dbName: String, dbUsername: String, dbPassword: String, querySource: String): DataFrame = {
    if (tableId == null || tableId.trim.isEmpty) throw new DataIOException(s"tableId cannot be null or empty")
    if (dbHost == null || dbHost.trim.isEmpty) throw new DataIOException(s"dbHost cannot be null or empty")
    if (dbPort == null || dbPort.trim.isEmpty) throw new DataIOException(s"dbPort cannot be null or empty")
    if (dbName == null || dbName.trim.isEmpty) throw new DataIOException(s"dbName cannot be null or empty")
    if (dbUsername == null || dbUsername.trim.isEmpty) throw new DataIOException(s"dbUsername cannot be null or empty")
    if (dbPassword == null || dbPassword.trim.isEmpty) throw new DataIOException(s"dbPassword cannot be null or empty")
    if (querySource == null || querySource.trim.isEmpty) throw new DataIOException(s"querySource cannot be null or empty")
    else try{
      val url = s"jdbc:postgresql://${dbHost}:${dbPort}/${dbName}"
      log.info(s"Reading ${tableId} table from ${url}.")
      val connectionProperties = new Properties()
      connectionProperties.setProperty("Driver", "org.postgresql.Driver")
      connectionProperties.put("user", dbUsername)
      connectionProperties.put("password", dbPassword)
      sparkSession.read.jdbc(url, querySource, connectionProperties)
    } catch {
      case x: Exception =>
        throw new DataIOException(s"Unable to read dataframe from paths $tableId - ${x.getClass}: ${x.getMessage}")
    }
  }
}
