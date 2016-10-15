package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

/**
  * This is responsible for the Merging part of the MR-BNL algorithm.
  *
  * @param bnlAlgorithm The core BNL algorithm
  */
private[bnl] class Merger(private val bnlAlgorithm: BnlAlgorithm) extends Serializable {

  private[bnl] def merge(flagsWithPoints: RDD[(Flag, Point)]): RDD[Point] = {
    val inSinglePartition = flagsWithPoints
      .coalesce(1)
      .glom()

    inSinglePartition.flatMap(bnlAlgorithm.computeSkylinesWithPreComparison(_))
  }
}
