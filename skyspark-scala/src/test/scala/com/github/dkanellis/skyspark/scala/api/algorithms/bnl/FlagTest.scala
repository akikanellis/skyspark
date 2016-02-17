package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import org.scalatest.FlatSpec

class FlagTest extends FlatSpec {

  val flag = new Flag(false, true, false, true) // 0101

  "A smaller than 0 index" should "throw IndexOutOfBoundsException" in {
    intercept[IndexOutOfBoundsException] {
      flag.getValueOf(-1)
    }
  }

  "A bigger than size index" should "throw IndexOutOfBoundsException" in {
    intercept[IndexOutOfBoundsException] {
      flag.getValueOf(4)
    }
  }

  "A 0 index" should "return the first value" in {
    assertResult(false)(flag.getValueOf(0))
  }

  "A 3 index" should "return the last value" in {
    assertResult(true)(flag.getValueOf(3))
  }
}
