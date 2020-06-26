package com.trial.common

import org.apache.spark.SparkConf

trait DefaultSparkConfProvider extends SparkConfProvider {
  this: ConfigProvider =>
  def getSparkConf(): SparkConf =
    new SparkConf()
      .set("spark.network.timeout", "1500s")
      .set("spark.broadcast.compress", "true")
      .set("spark.sql.broadcastTimeout", "36000")
      .set("spark.master", CONFIG_ENV.getString("trial.spark-master"))
}
