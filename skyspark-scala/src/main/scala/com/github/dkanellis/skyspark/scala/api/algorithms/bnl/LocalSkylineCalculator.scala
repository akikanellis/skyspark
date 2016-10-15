package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

/**
  * This is responsible for the Reduce task of the Division part of the MR-BNL algorithm.
  *
  * @param bnlAlgorithm The core BNL algorithm
  */
private[bnl] class LocalSkylineCalculator(private val bnlAlgorithm: BnlAlgorithm) extends Serializable {

  private[bnl] def computeLocalSkylines(flagPoints: RDD[(Flag, Point)]): RDD[(Flag, Point)] = {
    flagPoints
      .groupByKey
      .flatMapValues(bnlAlgorithm.computeSkylinesWithoutPreComparison)
  }
}
