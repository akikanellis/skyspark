package com.akikanellis.skyspark.scala.api.algorithms

import com.akikanellis.skyspark.scala.test_utils.UnitSpec

class PointTest extends UnitSpec {

  private var point: Point = _

  before {
    point = Point(5, 2, 7, 1)
  }

  "A smaller than 0 index" should "throw IndexOutOfBoundsException" in {
    an[IndexOutOfBoundsException] should be thrownBy point.dimension(-1)
  }

  "A bigger than size index" should "throw IndexOutOfBoundsException" in {
    an[IndexOutOfBoundsException] should be thrownBy point.dimension(4)
  }

  "A 0 index" should "return the first value" in {
    point.dimension(0) shouldBe 5
  }

  "A 3 index" should "return the last value" in {
    point.dimension(3) shouldBe 1
  }
}
