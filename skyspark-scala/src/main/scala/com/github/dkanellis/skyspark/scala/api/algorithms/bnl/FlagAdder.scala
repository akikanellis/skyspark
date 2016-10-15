package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

/**
  * Adds the corresponding flags to the points.
  *
  * @param medianFinder The median of the points
  */
private[bnl] class FlagAdder(private[bnl] val medianFinder: MedianFinder) extends Serializable {

  private[bnl] def this() = this(new MedianFinder)

  private[bnl] def addFlags(points: RDD[Point]): RDD[(Flag, Point)] = {
    val median = medianFinder.getMedian(points)
    val flagProducer = createFlagProducer(median)

    points.map(p => (flagProducer.calculateFlag(p), p))
  }

  protected[bnl] def createFlagProducer(median: Point): FlagProducer = new FlagProducer(median)
}
