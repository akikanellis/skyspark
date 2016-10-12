package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

private[bnl] class SkylineComputer(bnlAlgorithm: BnlAlgorithm) extends Serializable {

  private[bnl] def computeLocalSkylines(flagPoints: RDD[(Flag, Point)]): RDD[(Flag, Point)] = {
    flagPoints
      .groupByKey
      .flatMapValues(bnlAlgorithm.computeSkylinesWithoutPreComparison)
  }
}
