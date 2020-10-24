package com.analytic.entities

import com.analytic.entities.adapters.{GoogleCloudStorageOperator, SparkDataOperator, SparkSQLOperator, YAMLOperator}
import com.analytic.entities.exceptions.ControllerException
import com.analytic.entities.models.{EntityRequest, EntityRequestBuilder, InputEntityRequest, Response, Spark}
import com.analytic.entities.ports.{DataIOOperator, SQLOperator, TYAMLOperator}
import com.analytic.entities.services.EntityGeneratorService
import com.typesafe.scalalogging.Logger

object App extends Spark {
  private val log: Logger = Logger(classOf[App])

  def readRequestFile(path: String): InputEntityRequest = {
    import io.circe.generic.auto._
    log.info("Reading YAML request file")
    try {
      val yamlOperator: TYAMLOperator = new YAMLOperator with GoogleCloudStorageOperator
      yamlOperator.parseFileTo[InputEntityRequest](path)
    } catch {
      case x: Exception => throw new ControllerException(s"${x.getClass}: ${x.getMessage}")
    }
  }

  def makeRequestCore(
     inputRequestFile: InputEntityRequest,
     processDate: String,
     srcBkt: String,
     outputBkt: String): EntityRequest = try {
      log.info("Parsing request")
      EntityRequestBuilder(inputRequestFile, processDate, srcBkt, outputBkt)
    } catch {
      case x: Exception => throw new ControllerException(s"${x.getClass}: ${x.getMessage}")
    }

  def validateResponse(response: Response): Unit = if (!response.isValid)
    throw new ControllerException(s"${response.getType}: ${response.getValue}")

  def main(args: Array[String]): Unit = {
    val (yamlPath, processDate, srcBkt, outputBkt) = args match {
      case Array(path, date, bktIn, bktOut) => (path, date, bktIn, bktOut)
      case _ => throw new ControllerException("Invalid input arguments")
    }
    try {
      log.info(s"Starting Spark ${sparkSession.version}")
      val entityDetails: InputEntityRequest = readRequestFile(yamlPath)
      val request: EntityRequest = makeRequestCore(entityDetails, processDate, srcBkt, outputBkt)
      val sparkSqlOperator: SQLOperator = new SparkSQLOperator with Spark
      val avroOperator: DataIOOperator = new SparkDataOperator with Spark with GoogleCloudStorageOperator
      //val bigQueryOperator: TBigQueryOperator = new BigQueryOperator
      //val tablesRepository: TTableRepository = new TableRepository with GoogleCloudStorageOperator
      //val mainService = new EntityGeneratorService(avroOperator, sparkSqlOperator, bigQueryOperator, tablesRepository)
      //val response: Response = mainService.invoke(request)
      //validateResponse(response)
      log.info("Application finished correctly")
      sparkSession.stop()
    } catch {
      case t: Throwable => throw new ControllerException(s"${t.getClass}: ${t.getMessage}")
    }
  }

}
