package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class DividerTest extends FlatSpec with BeforeAndAfter with Matchers {

  private var points: RDD[Point] = _
  private var sc: SparkContext = _
  private var divider: Divider = _

  before {
    val pointsSeq = Seq(
      Point(5.4, 4.4), Point(5.0, 4.1), Point(3.6, 9.0), Point(5.9, 4.0),
      Point(5.9, 4.6), Point(2.5, 7.3), Point(6.3, 3.5), Point(9.9, 4.1),
      Point(6.7, 3.3), Point(6.1, 3.4))

    val sparkConf = new SparkConf().setAppName("MedianFinder tests").setMaster("local[*]")
    sc = new SparkContext(sparkConf)

    points = sc.parallelize(pointsSeq)

    divider = new Divider
    divider.numberOfDimensions = pointsSeq.head.size()
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "A zero dimension size" should "throw an IllegalStateException" in {
    divider.numberOfDimensions = 0

    an[IllegalStateException] should be thrownBy divider.divide(null)
  }

  "A negative dimension size" should "throw an IllegalStateException" in {
    divider.numberOfDimensions = -1

    an[IllegalStateException] should be thrownBy divider.divide(null)
  }

  "A set of points" should "return the local skylines with their flags" in {
    val expectedSkylinesWithFlags = Seq(
      (new Flag(true, true), Point(5.9, 4.6)),
      (new Flag(true, false), Point(5.0, 4.1)), (new Flag(true, false), Point(5.9, 4.0)),
      (new Flag(true, false), Point(6.7, 3.3)), (new Flag(true, false), Point(6.1, 3.4)),
      (new Flag(false, true), Point(2.5, 7.3)))


    val actualSkylinesWithFlags = divider.divide(points)

    actualSkylinesWithFlags.collect() should contain theSameElementsAs expectedSkylinesWithFlags
  }
}
