package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.api.helpers.Points

import scala.collection.mutable.ListBuffer

object BnlAlgorithm {

  def computeSkylinesWithoutPreComparison(pointIterable: Iterable[Point]) = {
    val localSkylines = ListBuffer[Point]()
    for (candidateSkyline <- pointIterable) {
      addDiscardOrDominate(localSkylines, candidateSkyline)
    }

    localSkylines
  }

  private def addDiscardOrDominate(localSkylines: ListBuffer[Point], candidateSkyline: Point) {
    for (pointToCheckAgainst <- localSkylines) {
      if (Points.dominates(pointToCheckAgainst, candidateSkyline)) {
        return
      } else if (Points.dominates(candidateSkyline, pointToCheckAgainst)) {
        localSkylines -= pointToCheckAgainst
      }
    }

    localSkylines += candidateSkyline
  }
}
