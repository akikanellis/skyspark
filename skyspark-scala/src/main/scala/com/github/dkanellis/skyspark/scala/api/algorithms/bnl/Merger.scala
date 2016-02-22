package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

private[bnl] class Merger extends Serializable {

  private val bnlAlgorithm = new BnlAlgorithm

  private[bnl] def merge(flagsWithPoints: RDD[(Flag, Point)]): RDD[Point] = {
    val inSinglePartition = flagsWithPoints
      .coalesce(1)
      .glom()

    inSinglePartition.flatMap(fp => bnlAlgorithm.computeSkylinesWithPreComparison(fp))
  }
}
