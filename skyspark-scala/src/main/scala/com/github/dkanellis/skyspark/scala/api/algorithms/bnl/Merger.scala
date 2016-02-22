package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

private[bnl] object Merger {

  private[bnl] def merge(flagsWithPoints: RDD[(Flag, Point)]): RDD[Point] = {
    val inSinglePartition = flagsWithPoints
      .coalesce(1)
      .glom()

    inSinglePartition.flatMap(fp => BnlAlgorithm.computeSkylinesWithPreComparison(fp))
  }
}
