package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.{Point, SkylineAlgorithm}
import org.apache.spark.rdd.RDD

class BlockNestedLoop extends SkylineAlgorithm {

  override def computeSkylinePoints(points: RDD[Point]): RDD[Point] = {
    val localSkylinesWithFlags = Divider.divide(points)

    Merger.merge(localSkylinesWithFlags)
  }
}
