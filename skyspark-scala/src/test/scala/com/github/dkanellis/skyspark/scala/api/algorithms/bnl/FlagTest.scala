package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FlagTest extends FlatSpec with BeforeAndAfter with Matchers {

  private var flag: Flag = _

  before {
    flag = Flag(false, true, false, true) // 0101
  }

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
    flag.bit(0) shouldBe false
  }

  "A 3 index" should "return the last value" in {
    flag.bit(3) shouldBe true
  }
}
