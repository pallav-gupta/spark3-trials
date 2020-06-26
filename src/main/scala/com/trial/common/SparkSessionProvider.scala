package com.trial.common

import org.apache.spark.joins.BroadCastHint
import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  this: SparkConfProvider =>
  def getSparkSession(APP_NAME: String): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(APP_NAME)
      .config(getSparkConf)
      .withExtensions { extensions =>
        extensions.injectResolutionRule(session => BroadCastHint)
      }
      .getOrCreate()

    spark
  }
}
