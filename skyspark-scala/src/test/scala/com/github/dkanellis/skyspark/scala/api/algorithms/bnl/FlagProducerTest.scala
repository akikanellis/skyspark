package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

class FlagProducerTest extends FlatSpec with BeforeAndAfterEach {

  val median = Point(5, 5)
  val flagProducer = new FlagProducer(median)

  "A point with x and y smaller than median" should "produce flag 00" in {
    val point = Point(4, 4)
    val expectedFlag = new Flag(false, false)

    val actualFlag = flagProducer.calculateFlag(point)

    assertResult(expectedFlag)(actualFlag)
  }

  "A point with x and y bigger than median" should "produce flag 11" in {
    val point = Point(6, 6)
    val expectedFlag = new Flag(true, true)

    val actualFlag = flagProducer.calculateFlag(point)

    assertResult(expectedFlag)(actualFlag)
  }

  "A point with x and y equal to median" should "produce flag 11" in {
    val point = Point(5, 5)
    val expectedFlag = new Flag(true, true)

    val actualFlag = flagProducer.calculateFlag(point)

    assertResult(expectedFlag)(actualFlag)
  }

  "A point with x bigger and y smaller than median" should "produce flag 10" in {
    val point = Point(6, 4)
    val expectedFlag = new Flag(true, false)

    val actualFlag = flagProducer.calculateFlag(point)

    assertResult(expectedFlag)(actualFlag)
  }

  "A point with x smaller and y bigger than median" should "produce flag 01" in {
    val point = Point(4, 6)
    val expectedFlag = new Flag(false, true)

    val actualFlag = flagProducer.calculateFlag(point)

    assertResult(expectedFlag)(actualFlag)
  }
}
