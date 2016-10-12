package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

private[bnl] class Divider(medianFinder: MedianFinder, bnlAlgorithm: BnlAlgorithm) extends Serializable {

  def this() = {
    this(new MedianFinder, new BnlAlgorithm)
  }

  private[bnl] def divide(points: RDD[Point]): RDD[(Flag, Point)] = {
    val median = medianFinder.getMedian(points)
    val flagProducer = createFlagProducer(median)

    val flagPoints = points.map(p => (flagProducer.calculateFlag(p), p))
    val groupedByFlag = flagPoints.groupByKey

    groupedByFlag.flatMapValues(bnlAlgorithm.computeSkylinesWithoutPreComparison)
  }

  protected def createFlagProducer(median: Point): FlagProducer = {
    new FlagProducer(median)
  }
}
