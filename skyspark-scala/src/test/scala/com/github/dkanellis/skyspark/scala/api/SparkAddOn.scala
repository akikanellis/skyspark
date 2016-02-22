package com.github.dkanellis.skyspark.scala.api

import org.apache.spark.{SparkConf, SparkContext}

trait SparkAddOn {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark-tests")

  def withSpark(f: SparkContext => Unit) = {
    val sc: SparkContext = new SparkContext(conf)
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }
}
