package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.{DominatingAlgorithm, Point}

import scala.collection.mutable.ListBuffer

private[bnl] class BnlAlgorithm extends Serializable {

  private[bnl] def computeSkylinesWithPreComparison(flagsWithPoints: Iterable[(Flag, Point)]): Iterable[Point] = {
    val localSkylines = ListBuffer[Point]()
    flagsWithPoints
      .filter(fp => passesPreComparison(fp._1))
      .map(_._2)
      .foreach(addDiscardOrDominate(localSkylines, _))

    localSkylines
  }

  private def passesPreComparison(flag: Flag) = {
    var passes = true
    for (i <- 0 until flag.size) {
      passes &= flag.bit(i)
    }

    !passes
  }

  private[bnl] def computeSkylinesWithoutPreComparison(pointIterable: Iterable[Point]): Iterable[Point] = {
    val localSkylines = ListBuffer[Point]()
    for (candidateSkyline <- pointIterable) {
      addDiscardOrDominate(localSkylines, candidateSkyline)
    }

    localSkylines
  }

  private def addDiscardOrDominate(localSkylines: ListBuffer[Point], candidateSkyline: Point) {
    for (pointToCheckAgainst <- localSkylines) {
      if (DominatingAlgorithm.dominates(pointToCheckAgainst, candidateSkyline)) {
        return
      } else if (DominatingAlgorithm.dominates(candidateSkyline, pointToCheckAgainst)) {
        localSkylines -= pointToCheckAgainst
      }
    }

    localSkylines += candidateSkyline
  }
}
