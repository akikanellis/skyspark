package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point

class FlagProducer(medianPointC: Point) extends Serializable {

  val medianPoint = medianPointC

  def calculateFlag(point: Point): Flag = {
    val bits = new Array[Boolean](point.size())
    for (i <- 0 until point.size) {
      bits(i) = point.dimension(i) >= medianPoint.dimension(i)
    }

    new Flag(bits: _*)
  }
}
