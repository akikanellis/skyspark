package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.scalatest.FlatSpec

class PointsTest extends FlatSpec {

  "A point with one dimension smaller" should "dominate when the rest dimensions are smaller or equal" in {
    val first = new Point(2, 5, 1)
    val second = new Point(3, 5, 6)

    assert(Points.dominates(first, second))
  }

  it should "not dominate when at least one dimension is bigger" in {
    val first = new Point(2, 5, 7)
    val second = new Point(3, 5, 6)

    assert(!Points.dominates(first, second))
  }

  "A point with all dimensions equal" should "not dominate" in {
    val first = new Point(2, 5, 4)
    val second = new Point(2, 5, 4)

    assert(!Points.dominates(first, second))
  }
}
