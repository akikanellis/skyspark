package com.akikanellis.skyspark.scala.api.algorithms

import com.akikanellis.skyspark.scala.test_utils.UnitSpec

class DominatingAlgorithmTest extends UnitSpec {

  "A point with one dimension smaller" should "dominate when the rest dimensions are smaller" in {
    val candidate = Point(2, 4, 5)
    val other = Point(3, 5, 6)

    DominatingAlgorithm.dominates(candidate, other) shouldBe true
  }

  it should "dominate when the rest dimensions are equal" in {
    val candidate = Point(2, 4, 5)
    val other = Point(3, 4, 5)

    DominatingAlgorithm.dominates(candidate, other) shouldBe true
  }

  it should "not dominate when at least one dimension is bigger" in {
    val candidate = Point(2, 4, 7)
    val other = Point(3, 5, 6)

    DominatingAlgorithm.dominates(candidate, other) shouldBe false
  }

  "A point with all dimensions bigger" should "not dominate" in {
    val candidate = Point(4, 6, 7)
    val other = Point(3, 5, 6)

    DominatingAlgorithm.dominates(candidate, other) shouldBe false
  }

  "A point with all dimensions equal" should "not dominate" in {
    val candidate = Point(2, 4, 5)
    val other = Point(2, 4, 5)

    DominatingAlgorithm.dominates(candidate, other) shouldBe false
  }
}
