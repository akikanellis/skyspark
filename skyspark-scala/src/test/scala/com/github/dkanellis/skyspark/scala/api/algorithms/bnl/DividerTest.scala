package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class DividerTest extends FlatSpec with BeforeAndAfter with Matchers with SparkAddOn {

  private var divider: Divider = _

  before {
    divider = new Divider
  }

  "A zero dimension size" should "throw an IllegalStateException" in {
    divider.numberOfDimensions = 0

    an[IllegalStateException] should be thrownBy divider.divide(null)
  }

  "A negative dimension size" should "throw an IllegalStateException" in {
    divider.numberOfDimensions = -1

    an[IllegalStateException] should be thrownBy divider.divide(null)
  }

  "A set of points" should "return the local skylines with their flags" in withSpark { sc =>
    val pointsSeq = Seq(
      Point(5.4, 4.4), Point(5.0, 4.1), Point(3.6, 9.0), Point(5.9, 4.0),
      Point(5.9, 4.6), Point(2.5, 7.3), Point(6.3, 3.5), Point(9.9, 4.1),
      Point(6.7, 3.3), Point(6.1, 3.4))

    val points = sc.parallelize(pointsSeq)
    divider.numberOfDimensions = pointsSeq.head.size()

    val expectedSkylinesWithFlags = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, false), Point(5.0, 4.1)), (Flag(true, false), Point(5.9, 4.0)),
      (Flag(true, false), Point(6.7, 3.3)), (Flag(true, false), Point(6.1, 3.4)),
      (Flag(false, true), Point(2.5, 7.3)))


    val actualSkylinesWithFlags = divider.divide(points)

    actualSkylinesWithFlags.collect() should contain theSameElementsAs expectedSkylinesWithFlags
  }
}
