package com.github.dkanellis.skyspark.api.algorithms

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.scalatest.FlatSpec

class PointTest extends FlatSpec {

  val point = new Point(5, 2, 7, 1)

  "A smaller than 1 index" should "throw IndexOutOfBoundsException" in {
    intercept[IndexOutOfBoundsException] {
      point.getValueOf(0)
    }
  }

  "A bigger than (size + 1) index" should "throw IndexOutOfBoundsException" in {
    intercept[IndexOutOfBoundsException] {
      point.getValueOf(5)
    }
  }

  "A 1 index" should "return the first value" in {
    assertResult(5)(point.getValueOf(1))
  }

  "A 4 index" should "return the last value" in {
    assertResult(1)(point.getValueOf(4))
  }
}
