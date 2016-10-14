package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.test_utils.UnitSpec

class TextFileToPointRddTest extends UnitSpec {

  "An empty line" should "throw an IllegalArgumentException" in {
    an[IllegalArgumentException] should be thrownBy TextFileToPointRdd.pointFromTextLine("", " ")
  }

  "A non-number character in the line" should "throw an IllegalArgumentException" in {
    an[IllegalArgumentException] should be thrownBy TextFileToPointRdd.pointFromTextLine("1 3 a", " ")
  }

  "A wrong delimiter" should "throw an IllegalArgumentException" in {
    an[IllegalArgumentException] should be thrownBy TextFileToPointRdd.pointFromTextLine("1 5", ",")
  }

  "A 1 number line" should "return a 1-dimensional point" in {
    val expectedPoint = Point(1)

    val actualPoint = TextFileToPointRdd.pointFromTextLine("1", " ")

    actualPoint shouldBe expectedPoint
  }

  "A 2 number line" should "return a 2-dimensional point" in {
    val expectedPoint = Point(1, 5)

    val actualPoint = TextFileToPointRdd.pointFromTextLine("1 5", " ")

    actualPoint shouldBe expectedPoint
  }

  "A floating point number line" should "return a floating point" in {
    val expectedPoint = Point(1.425214, 5.672734)

    val actualPoint = TextFileToPointRdd.pointFromTextLine("1.425214 5.672734", " ")

    actualPoint shouldBe expectedPoint
  }

  "A delimiter other than space" should "return the points" in {
    val expectedPoint = Point(5, 1)

    val actualPoint = TextFileToPointRdd.pointFromTextLine("5,1", ",")

    actualPoint shouldBe expectedPoint
  }
}
