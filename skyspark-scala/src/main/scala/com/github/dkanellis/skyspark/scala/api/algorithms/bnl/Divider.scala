package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

private[bnl] class Divider(flagAdder: FlagAdder, skylineComputer: SkylineComputer) extends Serializable {

  def this(bnlAlgorithm: BnlAlgorithm) = {
    this(new FlagAdder, new SkylineComputer(bnlAlgorithm))
  }

  private[bnl] def divide(points: RDD[Point]): RDD[(Flag, Point)] = {
    val flagPoints = flagAdder.addFlags(points)

    skylineComputer.computeLocalSkylines(flagPoints)
  }
}
