package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point

private[bnl] class FlagProducer(medianPointC: Point) extends Serializable {

  private val medianPoint = medianPointC

  private[bnl] def calculateFlag(point: Point): Flag = {
    val bits = new Array[Boolean](point.size())
    for (i <- 0 until point.size) {
      bits(i) = point.dimension(i) >= medianPoint.dimension(i)
    }

    new Flag(bits: _*)
  }
}
