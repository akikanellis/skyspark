package com.github.dkanellis.skyspark.scala.api.algorithms

import org.apache.spark.rdd.RDD

trait SkylineAlgorithm {
  def computeSkylinePoints(points: RDD[Point]): RDD[Point]
}
