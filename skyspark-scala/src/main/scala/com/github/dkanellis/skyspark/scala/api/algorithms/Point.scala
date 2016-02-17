package com.github.dkanellis.skyspark.scala.api.algorithms

import com.google.common.base.Preconditions

class Point(dimensionValuesC: Double*) {
  private val dimensionValues = dimensionValuesC

  def size() = dimensionValues.length

  def getValueOf(dimension: Int) = {
    Preconditions.checkPositionIndexes(1, dimensionValues.length, dimensionValues.length + 1)
    dimensionValues(dimension - 1)
  }

  override def equals(other: Any): Boolean = other match {
    case that: Point => (that canEqual this) && dimensionValues == that.dimensionValues
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Point]

  override def hashCode(): Int = {
    val state = Seq(dimensionValues)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
