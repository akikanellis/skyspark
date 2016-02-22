package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers, PrivateMethodTester}

class MedianFinderTest extends FlatSpec with PrivateMethodTester with BeforeAndAfter with Matchers {

  private var sc: SparkContext = _
  private var medianFinder: MedianFinder = _

  before {
    val sparkConf = new SparkConf().setAppName("MedianFinder tests").setMaster("local[*]")
    sc = new SparkContext(sparkConf)

    medianFinder = new MedianFinder
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "A zero dimension size" should "throw an IllegalStateException" in {
    medianFinder.numberOfDimensions = 0

    an[IllegalStateException] should be thrownBy medianFinder.getMedian(null)
  }

  "A negative dimension size" should "throw an IllegalStateException" in {
    medianFinder.numberOfDimensions = -1

    an[IllegalStateException] should be thrownBy medianFinder.getMedian(null)
  }

  "2D points" should "produce correct median" in {
    val pointsArray = Seq[Point](new Point(1, 1), new Point(3, 2), new Point(4, 7), new Point(9, 5))
    val points = sc.parallelize(pointsArray)
    medianFinder.numberOfDimensions = pointsArray.head.size()
    val expectedMedian = new Point(4.5, 3.5)

    val actualMedian = medianFinder.getMedian(points)

    assertResult(expectedMedian)(actualMedian)
  }

  "3D points" should "produce correct median" in {
    val pointsArray = Seq[Point](new Point(1, 1, 5), new Point(3, 2, 3), new Point(4, 7, 1), new Point(9, 5, 8))
    val points = sc.parallelize(pointsArray)
    medianFinder.numberOfDimensions = pointsArray.head.size()
    val expectedMedian = new Point(4.5, 3.5, 4)

    val actualMedian = medianFinder.getMedian(points)

    assertResult(expectedMedian)(actualMedian)
  }
}
