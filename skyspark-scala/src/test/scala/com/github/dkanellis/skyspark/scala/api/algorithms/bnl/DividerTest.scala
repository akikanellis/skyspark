package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class DividerTest extends FlatSpec with BeforeAndAfter with Matchers {

  private var points: RDD[Point] = _
  private var sc: SparkContext = _

  before {
    val pointsSeq = Seq(
      new Point(5.4, 4.4), new Point(5.0, 4.1), new Point(3.6, 9.0), new Point(5.9, 4.0),
      new Point(5.9, 4.6), new Point(2.5, 7.3), new Point(6.3, 3.5), new Point(9.9, 4.1),
      new Point(6.7, 3.3), new Point(6.1, 3.4))

    val sparkConf = new SparkConf().setAppName("MedianFinder tests").setMaster("local[*]")
    sc = new SparkContext(sparkConf)

    points = sc.parallelize(pointsSeq)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "A set of points" should "return the local skylines with their flags" in {
    val expectedSkylinesWithFlags = Seq(
      (new Flag(true, true), new Point(5.9, 4.6)), (new Flag(true, false), new Point(5.0, 4.1)),
      (new Flag(true, false), new Point(5.9, 4.0)), (new Flag(false, true), new Point(2.5, 7.3)),
      (new Flag(true, false), new Point(6.7, 3.3)), (new Flag(true, false), new Point(6.1, 3.4)))

    val actualSkylinesWithFlags = Divider.divide(points)

    actualSkylinesWithFlags.collect() should contain theSameElementsAs expectedSkylinesWithFlags
  }
}
