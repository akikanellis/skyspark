package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, PrivateMethodTester}

class BlockNestedLoopTest extends FlatSpec with PrivateMethodTester {

  val sparkContext = new SparkContext(new SparkConf().setAppName("BlockNestedLoop tests").setMaster("local[*]"))

  "2D points" should "produce correct median" in {
    val pointsArray = Array(new Point(1, 1), new Point(3, 2), new Point(4, 7), new Point(9, 5))
    val points: RDD[Point] = sparkContext.parallelize(pointsArray)
    val expectedMedian = new Point(4.5, 3.5)

    val bnl = new BlockNestedLoop
    val decorateGetMedian = PrivateMethod[Point]('getMedian)
    val actualMedian = bnl invokePrivate decorateGetMedian(points)

    assertResult(expectedMedian)(actualMedian)
  }
}
