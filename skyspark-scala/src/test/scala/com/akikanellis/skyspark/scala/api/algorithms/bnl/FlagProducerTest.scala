package com.akikanellis.skyspark.scala.api.algorithms.bnl

import com.akikanellis.skyspark.scala.api.algorithms.Point
import com.akikanellis.skyspark.scala.test_utils.UnitSpec

class FlagProducerTest extends UnitSpec {
  var median: Point = _
  var flagProducer: FlagProducer = _

  before {
    median = Point(5, 5)
    flagProducer = new FlagProducer(median)
  }

  "A point with x and y smaller than median" should "produce flag 00" in {
    val point = Point(4, 4)
    val expectedFlag = Flag(false, false)

    val actualFlag = flagProducer.calculateFlag(point)

    actualFlag shouldBe expectedFlag
  }

  "A point with x and y bigger than median" should "produce flag 11" in {
    val point = Point(6, 6)
    val expectedFlag = Flag(true, true)

    val actualFlag = flagProducer.calculateFlag(point)

    actualFlag shouldBe expectedFlag
  }

  "A point with x and y equal to median" should "produce flag 11" in {
    val point = Point(5, 5)
    val expectedFlag = Flag(true, true)

    val actualFlag = flagProducer.calculateFlag(point)

    actualFlag shouldBe expectedFlag
  }

  "A point with x bigger and y smaller than median" should "produce flag 10" in {
    val point = Point(6, 4)
    val expectedFlag = Flag(true, false)

    val actualFlag = flagProducer.calculateFlag(point)

    actualFlag shouldBe expectedFlag
  }

  "A point with x smaller and y bigger than median" should "produce flag 01" in {
    val point = Point(4, 6)
    val expectedFlag = Flag(false, true)

    val actualFlag = flagProducer.calculateFlag(point)

    actualFlag shouldBe expectedFlag
  }
}
