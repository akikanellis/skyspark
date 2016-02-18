package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point

class FlagProducer(medianPointC: Point) extends Serializable {

  val medianPoint = medianPointC

  def calculateFlag(point: Point) = {
    val bits = new Array[Boolean](point.size())
    for (i <- 0 until point.size) {
      bits(i) = point.getValueOf(i) >= medianPoint.getValueOf(i)
    }

    new Flag(bits: _*)
  }
}
