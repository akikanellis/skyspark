package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.google.common.base.Preconditions
import org.apache.spark.rdd.RDD

private[bnl] class MedianFinder extends Serializable {

  private[bnl] var numberOfDimensions = 0

  private[bnl] def getMedian(points: RDD[Point]): Point = {
    Preconditions.checkState(numberOfDimensions > 0, "Dimensionality can't be less than 1.", null)

    val medianDimensionValues = new Array[Double](numberOfDimensions)
    for (i <- 0 until numberOfDimensions) {
      medianDimensionValues(i) = getMaxValueOfDimension(points, i) / 2
    }

    Point(medianDimensionValues: _*)
  }

  private def getMaxValueOfDimension(points: RDD[Point], dimensionIndex: Int) = {
    val biggestPointByDimension = points.max()(new Ordering[Point]() {
      override def compare(first: Point, second: Point): Int =
        Ordering[Double].compare(first.dimension(dimensionIndex), second.dimension(dimensionIndex))
    })

    biggestPointByDimension.dimension(dimensionIndex)
  }
}
