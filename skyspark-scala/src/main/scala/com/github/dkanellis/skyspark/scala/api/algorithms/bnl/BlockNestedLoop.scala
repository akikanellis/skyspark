package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.{Point, SkylineAlgorithm}
import org.apache.spark.rdd.RDD

class BlockNestedLoop extends SkylineAlgorithm {

  var flagProducer: FlagProducer = _

  override def computeSkylinePoints(points: RDD[Point]): RDD[Point] = {
    flagProducer = new FlagProducer(getMedian(points))

    throw new UnsupportedOperationException("WIP")
  }

  private def getMedian(points: RDD[Point]) = {
    val dimensions = points.take(1)(0).size()

    val medianDimensionValues = new Array[Double](dimensions)
    for (i <- 0 until dimensions) {
      medianDimensionValues(i) = points.max()(new Ordering[Point]() {
        override def compare(first: Point, second: Point): Int =
          Ordering[Double].compare(first.getValueOf(i), second.getValueOf(i))
      }).getValueOf(i)
    }

    new Point(medianDimensionValues: _*)
  }
}
