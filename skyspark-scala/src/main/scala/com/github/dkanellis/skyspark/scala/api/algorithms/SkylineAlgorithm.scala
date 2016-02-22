package com.github.dkanellis.skyspark.scala.api.algorithms

import org.apache.spark.rdd.RDD

trait SkylineAlgorithm extends Serializable {

  def computeSkylinePoints(points: RDD[Point]): RDD[Point]
}
