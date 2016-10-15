package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import javax.annotation.concurrent.ThreadSafe

import com.github.dkanellis.skyspark.scala.api.algorithms.{Point, SkylineAlgorithm}
import org.apache.spark.rdd.RDD

/**
  * A MapReduce based, block-nested loop algorithm (MR-BNL).
  * <p>
  * First we do the division job of the points to <Flag, Point> and then we merge the local skylines together and
  * calculate the total skyline set.
  * <p>
  * For more information read <i>Adapting Skyline Computation to the MapReduce Framework: Algorithms and Experiments</i>
  * by <i>Boliang Zhang, Shuigeng Zhou, Jihong Guan</i>
  *
  * @param divider The Division part of the algorithm
  * @param merger  The Merging part of the algorithm
  */
@ThreadSafe
class BlockNestedLoop private[bnl](private val divider: Divider, private val merger: Merger) extends SkylineAlgorithm {

  def this() = this(new BnlAlgorithm)

  override def computeSkylinePoints(points: RDD[Point]): RDD[Point] = {
    require(!points.isEmpty)

    val localSkylinesWithFlags = divider.divide(points)

    merger.merge(localSkylinesWithFlags)
  }

  private def this(bnlAlgorithm: BnlAlgorithm) = this(new Divider(bnlAlgorithm), new Merger(bnlAlgorithm))
}
