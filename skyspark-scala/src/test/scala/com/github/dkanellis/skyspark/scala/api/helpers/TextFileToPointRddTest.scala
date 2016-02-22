package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

class TextFileToPointRddTest extends FlatSpec with PrivateMethodTester with Matchers {

  val decoratePointFromTextLine = PrivateMethod[Point]('pointFromTextLine)

  "An empty line" should "throw an IllegalArgumentException" in {
    an[IllegalArgumentException] should be thrownBy (TextFileToPointRdd invokePrivate decoratePointFromTextLine("", " "))
  }

  "A non-number character in the line" should "throw an IllegalArgumentException" in {
    an[IllegalArgumentException] should be thrownBy (TextFileToPointRdd invokePrivate decoratePointFromTextLine("1 3 a", " "))
  }

  "A wrong delimiter" should "throw an IllegalArgumentException" in {
    an[IllegalArgumentException] should be thrownBy (TextFileToPointRdd invokePrivate decoratePointFromTextLine("1 5", ","))
  }

  "A 1 number line" should "return a 1-dimensional point" in {
    val expected = Point(1)
    val actual = TextFileToPointRdd invokePrivate decoratePointFromTextLine("1", " ")

    assertResult(expected)(actual)
  }

  "A 2 number line" should "return a 2-dimensional point" in {
    val expected = Point(1, 5)
    val actual = TextFileToPointRdd invokePrivate decoratePointFromTextLine("1 5", " ")

    assertResult(expected)(actual)
  }
}
