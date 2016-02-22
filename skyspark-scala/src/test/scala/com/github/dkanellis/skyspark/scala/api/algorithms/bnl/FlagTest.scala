package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import org.scalatest.FlatSpec

class FlagTest extends FlatSpec {

  val flag = Flag(false, true, false, true) // 0101

  "A smaller than 0 index" should "throw IndexOutOfBoundsException" in {
    intercept[IndexOutOfBoundsException] {
      flag.bit(-1)
    }
  }

  "A bigger than size index" should "throw IndexOutOfBoundsException" in {
    intercept[IndexOutOfBoundsException] {
      flag.bit(4)
    }
  }

  "A 0 index" should "return the first value" in {
    assertResult(false)(flag.bit(0))
  }

  "A 3 index" should "return the last value" in {
    assertResult(true)(flag.bit(3))
  }
}
