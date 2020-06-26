package com.trial.common

import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.util.Properties

trait ConfigProvider {
  val environment: String = Properties.envOrElse("CONFIG_FILE", "dev")
  val CONFIG_ENV = ConfigFactory.load(environment)
  val workflowPropertyMap: mutable.Map[String, String] = mutable.Map()
}
