package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.test_utils.UnitSpec

class PointsTest extends UnitSpec {

  "A point with one dimension smaller" should "dominate when the rest dimensions are smaller or equal" in {
    val first = Point(2, 5, 1)
    val second = Point(3, 5, 6)

    Points.dominates(first, second) shouldBe true
  }

  it should "not dominate when at least one dimension is bigger" in {
    val first = Point(2, 5, 7)
    val second = Point(3, 5, 6)

    Points.dominates(first, second) shouldBe false
  }

  "A point with all dimensions equal" should "not dominate" in {
    val first = Point(2, 5, 4)
    val second = Point(2, 5, 4)

    Points.dominates(first, second) shouldBe false
  }
}
