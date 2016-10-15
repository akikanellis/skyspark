package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

/**
  * Divides the data into appropriate partitions by using their flags and computes the local skylines for each
  * partition using the skyline computer.
  *
  * @param flagAdder              Adds the appropriate flags to the points
  * @param localSkylineCalculator Computes the local skylines
  */
private[bnl] class Divider(private val flagAdder: FlagAdder, private val localSkylineCalculator: LocalSkylineCalculator)
  extends Serializable {

  private[bnl] def this(bnlAlgorithm: BnlAlgorithm) = this(new FlagAdder, new LocalSkylineCalculator(bnlAlgorithm))

  private[bnl] def divide(points: RDD[Point]): RDD[(Flag, Point)] = {
    val flagPoints = flagAdder.addFlags(points)

    localSkylineCalculator.computeLocalSkylines(flagPoints)
  }
}
