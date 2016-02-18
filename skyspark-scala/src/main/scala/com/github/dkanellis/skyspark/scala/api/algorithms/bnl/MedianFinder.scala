package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

object MedianFinder {

  def getMedian(points: RDD[Point], numOfDimensions: Int) = {
    val medianDimensionValues = new Array[Double](numOfDimensions)
    for (i <- 0 until numOfDimensions) {
      medianDimensionValues(i) = getMaxValueOfDimension(points, i) / 2
    }

    new Point(medianDimensionValues: _*)
  }

  private def getMaxValueOfDimension(points: RDD[Point], i: Int) = {
    val biggestPointByDimension = points.max()(new Ordering[Point]() {
      override def compare(first: Point, second: Point): Int =
        Ordering[Double].compare(first.getValueOf(i), second.getValueOf(i))
    })

    biggestPointByDimension.getValueOf(i)
  }
}
