package com.analytic.entities.models

import org.apache.spark.sql.SparkSession, org.apache.spark.SparkConf

trait Spark extends Serializable {
  lazy val sparkSession: SparkSession = SparkSession.builder.config({
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Pipeline")
    sparkConf.setAll(Seq(
      ("spark.debug.maxToStringField", "200"),
      ("spark.network.timeout", "600s"),
      ("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    ))
  }).getOrCreate
}
