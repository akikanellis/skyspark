package com.github.dkanellis.skyspark.scala.api.algorithms

import com.google.common.base.Preconditions

class Point(dimensionValuesC: Double*) {
  val dimensionValues = dimensionValuesC

  def getValueOf(dimension: Int) = {
    Preconditions.checkPositionIndexes(1, dimensionValues.length, dimensionValues.length + 1)
    dimensionValues(dimension - 1)
  }
}
