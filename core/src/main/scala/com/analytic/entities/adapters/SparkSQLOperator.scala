package com.analytic.entities.adapters

import com.analytic.entities.exceptions.SparkException
import com.analytic.entities.models.Spark
import com.analytic.entities.ports.SQLOperator
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame

class SparkSQLOperator extends SQLOperator { self: Spark =>
  private val log: Logger = Logger(classOf[SparkSQLOperator])

  def createTempView(df:DataFrame)(viewName: String): Unit = {
    if (viewName == null) throw new SparkException("viewName cannot be null")
    else try df.createTempView(viewName)
    catch {
      case x: Exception => throw new SparkException(s"Error creating temporal view - ${x.getClass}: ${x.getMessage}")
    }
  }

  def executeQuery(query: String): DataFrame =
    if (query == null) throw new SparkException(s"query cannot be null")
    else try {
      log.info(s"Executing query: $query")
      sparkSession.sql(query)
    } catch {
      case x: Exception => throw new SparkException(s"Error querying the table - ${x.getClass}: ${x.getMessage}")
    }

}
