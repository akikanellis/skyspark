package com.akikanellis.skyspark.scala.api.algorithms

import org.apache.spark.rdd.RDD

/**
  * A skyline algorithm is an algorithm capable of calculating the skyline set of a dataset S. Current implementations
  * include [[com.akikanellis.skyspark.scala.api.algorithms.bnl.BlockNestedLoop]].
  */
trait SkylineAlgorithm extends Serializable {

  /**
    * Given an RDD of points, return their skyline set.
    *
    * @param points the points to compute the skyline for.
    * @return the skyline set.
    */
  def computeSkylinePoints(points: RDD[Point]): RDD[Point]
}
