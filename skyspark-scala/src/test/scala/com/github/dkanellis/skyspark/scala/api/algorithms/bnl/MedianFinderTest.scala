package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers, PrivateMethodTester}

class MedianFinderTest extends FlatSpec with PrivateMethodTester with BeforeAndAfter with Matchers with SparkAddOn {

  private var medianFinder: MedianFinder = _

  before {
    medianFinder = new MedianFinder
  }

  "2D points" should "produce correct median" in withSpark { sc =>
    val pointsArray = Seq[Point](Point(1, 1), Point(3, 2), Point(4, 7), Point(9, 5))
    val points = sc.parallelize(pointsArray)
    medianFinder.numberOfDimensions = pointsArray.head.size()
    val expectedMedian = Point(4.5, 3.5)

    val actualMedian = medianFinder.getMedian(points)

    expectedMedian shouldBe actualMedian
  }

  "3D points" should "produce correct median" in withSpark { sc =>
    val pointsSeq = Seq[Point](Point(1, 1, 5), Point(3, 2, 3), Point(4, 7, 1), Point(9, 5, 8))
    val points = sc.parallelize(pointsSeq)
    medianFinder.numberOfDimensions = pointsSeq.head.size()
    val expectedMedian = Point(4.5, 3.5, 4)

    val actualMedian = medianFinder.getMedian(points)

    expectedMedian shouldBe actualMedian
  }
}
