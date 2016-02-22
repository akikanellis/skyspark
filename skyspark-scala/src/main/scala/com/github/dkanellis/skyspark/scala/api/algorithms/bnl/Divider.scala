package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.google.common.base.Preconditions
import org.apache.spark.rdd.RDD

private[bnl] class Divider {

  private[bnl] var numberOfDimensions = 0

  private[bnl] def divide(points: RDD[Point]): RDD[(Flag, Point)] = {
    Preconditions.checkState(numberOfDimensions > 0, "Dimensionality can't be less than 1.", null)

    val median = MedianFinder.getMedian(points, numberOfDimensions)
    val flagProducer = new FlagProducer(median)

    val flagPoints = points.map(p => (flagProducer.calculateFlag(p), p))
    val groupedByFlag = flagPoints.groupByKey()

    groupedByFlag.flatMapValues(BnlAlgorithm.computeSkylinesWithoutPreComparison)
  }
}
