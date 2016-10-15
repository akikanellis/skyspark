package com.akikanellis.skyspark.scala.test_utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

trait SparkAddOn {
  private val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-tests")

  def withSpark(f: SparkContext => Unit) = {
    setLogLevels(Level.WARN)
    val sc: SparkContext = new SparkContext(conf)

    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }

  private def setLogLevels(level: Level) {
    Logger.getRootLogger.setLevel(level)
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }
}
