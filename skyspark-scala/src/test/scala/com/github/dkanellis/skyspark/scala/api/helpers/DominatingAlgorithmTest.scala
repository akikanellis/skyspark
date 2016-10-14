package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.{DominatingAlgorithm, Point}
import com.github.dkanellis.skyspark.scala.test_utils.UnitSpec

class DominatingAlgorithmTest extends UnitSpec {

  "A point with one dimension smaller" should "dominate when the rest dimensions are smaller" in {
    val dominator = Point(2, 4, 5)
    val dominatable = Point(3, 5, 6)

    DominatingAlgorithm.dominates(dominator, dominatable) shouldBe true
  }

  it should "dominate when the rest dimensions are equal" in {
    val first = Point(2, 4, 5)
    val second = Point(3, 4, 5)

    DominatingAlgorithm.dominates(first, second) shouldBe true
  }

  it should "not dominate when at least one dimension is bigger" in {
    val first = Point(2, 4, 7)
    val second = Point(3, 5, 6)

    DominatingAlgorithm.dominates(first, second) shouldBe false
  }

  "A point with all dimensions bigger" should "not dominate" in {
    val first = Point(4, 6, 7)
    val second = Point(3, 5, 6)

    DominatingAlgorithm.dominates(first, second) shouldBe false
  }

  "A point with all dimensions equal" should "not dominate" in {
    val first = Point(2, 4, 5)
    val second = Point(2, 4, 5)

    DominatingAlgorithm.dominates(first, second) shouldBe false
  }
}
