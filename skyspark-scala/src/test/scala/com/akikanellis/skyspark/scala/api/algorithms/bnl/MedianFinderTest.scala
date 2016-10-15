package com.akikanellis.skyspark.scala.api.algorithms.bnl

import com.akikanellis.skyspark.scala.api.algorithms.Point
import com.akikanellis.skyspark.scala.test_utils.{SparkAddOn, UnitSpec}
import org.apache.spark.SparkContext

class MedianFinderTest extends UnitSpec with SparkAddOn {
  private var medianFinder: MedianFinder = _

  before {
    medianFinder = new MedianFinder
  }

  "A single point" should "produce correct median" in withSpark { sc =>
    val points = Seq(Point(1))
    val expectedMedian = Point(0.5)

    assertMedianIsCorrect(sc, points, expectedMedian)
  }

  "1D points" should "produce correct median" in withSpark { sc =>
    val points = Seq(Point(1), Point(3), Point(4), Point(9))
    val expectedMedian = Point(4.5)

    assertMedianIsCorrect(sc, points, expectedMedian)
  }

  "2D points" should "produce correct median" in withSpark { sc =>
    val points = Seq(Point(1, 1), Point(3, 2), Point(4, 7), Point(9, 5))
    val expectedMedian = Point(4.5, 3.5)

    assertMedianIsCorrect(sc, points, expectedMedian)
  }

  "3D points" should "produce correct median" in withSpark { sc =>
    val points = Seq(Point(1, 1, 5), Point(3, 2, 3), Point(4, 7, 1), Point(9, 5, 8))
    val expectedMedian = Point(4.5, 3.5, 4)

    assertMedianIsCorrect(sc, points, expectedMedian)
  }

  private def assertMedianIsCorrect(sc: SparkContext, pointsSeq: Seq[Point], expectedMedian: Point) {
    val points = sc.parallelize(pointsSeq)

    val actualMedian = medianFinder.getMedian(points)

    expectedMedian shouldEqual actualMedian
  }
}
