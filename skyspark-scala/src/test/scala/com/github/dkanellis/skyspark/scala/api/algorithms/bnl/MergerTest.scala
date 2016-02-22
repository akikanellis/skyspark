package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class MergerTest extends FlatSpec with BeforeAndAfter with Matchers {

  private var flagPoints: RDD[(Flag, Point)] = _
  private var sc: SparkContext = _
  private var merger: Merger = _

  before {
    val flagPointsSeq = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, false), Point(5.0, 4.1)), (Flag(true, false), Point(5.9, 4.0)),
      (Flag(true, false), Point(6.7, 3.3)), (Flag(true, false), Point(6.1, 3.4)),
      (Flag(false, true), Point(2.5, 7.3)))

    val sparkConf = new SparkConf().setAppName("MedianFinder tests").setMaster("local[*]")
    sc = new SparkContext(sparkConf)

    flagPoints = sc.parallelize(flagPointsSeq)

    merger = new Merger
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "A set of flag-points" should "keep only the skylines and be merged" in {
    val expectedSkylines = Seq(
      Point(5.0, 4.1), Point(5.9, 4.0), Point(2.5, 7.3), Point(6.7, 3.3),
      Point(6.1, 3.4))

    val actualSkylines = merger.merge(flagPoints)

    actualSkylines.collect() should contain theSameElementsAs expectedSkylines
  }
}
