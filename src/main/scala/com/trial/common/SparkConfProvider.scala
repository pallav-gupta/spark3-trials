package com.trial.common

import org.apache.spark.SparkConf

trait SparkConfProvider {
  def getSparkConf(): SparkConf
}
