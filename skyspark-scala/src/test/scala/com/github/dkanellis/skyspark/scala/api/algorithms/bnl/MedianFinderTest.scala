package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers, PrivateMethodTester}

class MedianFinderTest extends FlatSpec with PrivateMethodTester with BeforeAndAfter with Matchers with SparkAddOn {

  private var medianFinder: MedianFinder = _

  before {
    medianFinder = new MedianFinder
  }

  "A single point" should "produce correct median" in withSpark { sc =>
    val pointsArray = Seq[Point](Point(1))
    val expectedMedian = Point(0.5)

    assertMedianIsCorrect(sc, pointsArray, expectedMedian)
  }

  "1D points" should "produce correct median" in withSpark { sc =>
    val pointsArray = Seq[Point](Point(1), Point(3), Point(4), Point(9))
    val expectedMedian = Point(4.5)

    assertMedianIsCorrect(sc, pointsArray, expectedMedian)
  }

  "2D points" should "produce correct median" in withSpark { sc =>
    val pointsArray = Seq[Point](Point(1, 1), Point(3, 2), Point(4, 7), Point(9, 5))
    val expectedMedian = Point(4.5, 3.5)

    assertMedianIsCorrect(sc, pointsArray, expectedMedian)
  }

  "3D points" should "produce correct median" in withSpark { sc =>
    val pointsArray = Seq[Point](Point(1, 1, 5), Point(3, 2, 3), Point(4, 7, 1), Point(9, 5, 8))
    val expectedMedian = Point(4.5, 3.5, 4)

    assertMedianIsCorrect(sc, pointsArray, expectedMedian)
  }

  private def assertMedianIsCorrect(sc: SparkContext, pointsArray: Seq[Point], expectedMedian: Point): Unit = {
    val points = sc.parallelize(pointsArray)

    val actualMedian = medianFinder.getMedian(points)

    expectedMedian shouldBe actualMedian
  }
}
