package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

private[bnl] class FlagAdder(medianFinder: MedianFinder) extends Serializable {

  def this() {
    this(new MedianFinder)
  }

  private[bnl] def addFlags(points: RDD[Point]): RDD[(Flag, Point)] = {
    val median = medianFinder.getMedian(points)
    val flagProducer = createFlagProducer(median)

    points.map(p => (flagProducer.calculateFlag(p), p))
  }

  protected def createFlagProducer(median: Point): FlagProducer = {
    new FlagProducer(median)
  }
}
