package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.{Point, SkylineAlgorithm}
import org.apache.spark.rdd.RDD

class BlockNestedLoop(divider: Divider, merger: Merger) extends SkylineAlgorithm {

  def this() = {
    this(new Divider, new Merger)
  }

  override def computeSkylinePoints(points: RDD[Point]): RDD[Point] = {
    divider.numberOfDimensions = points.first().size()

    val localSkylinesWithFlags = divider.divide(points)

    merger.merge(localSkylinesWithFlags)
  }
}
