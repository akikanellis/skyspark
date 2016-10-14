package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.{Point, SkylineAlgorithm}
import org.apache.spark.rdd.RDD

/**
  * The block-nested loop (BNL) algorithm.
  * <p>
  * First we do the division job of the points to <Flag, Point> and then we merge the local skylines together and
  * calculate the total skyline set.
  * <p>
  * For more information read <i>Adapting Skyline Computation to the MapReduce Framework: Algorithms and Experiments</i>
  * by <i>Boliang Zhang, Shuigeng Zhou, Jihong Guan</i>
  */
class BlockNestedLoop(private val divider: Divider, private val merger: Merger) extends SkylineAlgorithm {

  def this() = this(new Divider(new BnlAlgorithm), new Merger)

  override def computeSkylinePoints(points: RDD[Point]): RDD[Point] = {
    require(!points.isEmpty)

    val localSkylinesWithFlags = divider.divide(points)

    merger.merge(localSkylinesWithFlags)
  }
}
