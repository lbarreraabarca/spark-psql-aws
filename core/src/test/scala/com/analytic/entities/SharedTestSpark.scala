package com.analytic.entities

import com.analytic.entities.models.Spark
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SharedTestSpark extends Spark {
  override lazy val sparkSession: SparkSession = SparkSession.builder
    .appName("Test")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate
  val loadAvro: String => DataFrame = sparkSession.read.format("avro").load(_)
}
