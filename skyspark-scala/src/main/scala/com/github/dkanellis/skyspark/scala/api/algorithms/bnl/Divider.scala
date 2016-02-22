package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.google.common.base.Preconditions
import org.apache.spark.rdd.RDD

private[bnl] class Divider extends Serializable {

  private val medianFinder = new MedianFinder
  private val bnlAlgorithm = new BnlAlgorithm
  private[bnl] var numberOfDimensions = 0

  private[bnl] def divide(points: RDD[Point]): RDD[(Flag, Point)] = {
    Preconditions.checkState(numberOfDimensions > 0, "Dimensionality can't be less than 1.", null)

    medianFinder.numberOfDimensions = numberOfDimensions
    val median = medianFinder.getMedian(points)
    val flagProducer = new FlagProducer(median)

    val flagPoints = points.map(p => (flagProducer.calculateFlag(p), p))
    val groupedByFlag = flagPoints.groupByKey()

    groupedByFlag.flatMapValues(bnlAlgorithm.computeSkylinesWithoutPreComparison)
  }
}
