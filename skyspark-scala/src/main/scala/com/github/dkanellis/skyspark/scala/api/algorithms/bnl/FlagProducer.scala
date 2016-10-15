package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point

/**
  * Calculates the corresponding flag for a point by comparing it to the median.
  *
  * @param medianPoint The median point to compare with
  */
private[bnl] class FlagProducer(private val medianPoint: Point) extends Serializable {

  private[bnl] def calculateFlag(point: Point): Flag = {
    val bits = point.dimensions
      .zip(medianPoint.dimensions)
      .map { case (pointDimension, medianDimension) => isPointWorseThanMedian(pointDimension, medianDimension) }
      .toArray

    Flag(bits: _*)
  }

  private def isPointWorseThanMedian(pointDimension: Double, medianDimension: Double): Boolean =
    pointDimension >= medianDimension
}
