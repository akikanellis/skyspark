package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, PrivateMethodTester}

class MedianFinderTest extends FlatSpec with PrivateMethodTester with BeforeAndAfter {

  private var sc: SparkContext = _

  before {
    val sparkConf = new SparkConf().setAppName("MedianFinder tests").setMaster("local[*]")
    sc = new SparkContext(sparkConf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "2D points" should "produce correct median" in {
    val pointsArray = Seq[Point](new Point(1, 1), new Point(3, 2), new Point(4, 7), new Point(9, 5))
    val points = sc.parallelize(pointsArray)
    val expectedMedian = new Point(4.5, 3.5)

    val actualMedian = MedianFinder.getMedian(points, 2)

    assertResult(expectedMedian)(actualMedian)
  }

  "3D points" should "produce correct median" in {
    val pointsArray = Seq[Point](new Point(1, 1, 5), new Point(3, 2, 3), new Point(4, 7, 1), new Point(9, 5, 8))
    val points = sc.parallelize(pointsArray)
    val expectedMedian = new Point(4.5, 3.5, 4)

    val actualMedian = MedianFinder.getMedian(points, 3)

    assertResult(expectedMedian)(actualMedian)
  }
}
