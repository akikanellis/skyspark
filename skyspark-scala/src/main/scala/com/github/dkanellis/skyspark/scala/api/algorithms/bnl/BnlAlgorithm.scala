package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.api.helpers.Points

import scala.collection.mutable.ListBuffer

object BnlAlgorithm {

  def computeSkylinesWithPreComparison(flagsWithPoints: Iterable[(Flag, Point)]): Iterable[Point] = {
    val localSkylines = ListBuffer[Point]()
    flagsWithPoints
      .filter(fp => passesPreComparison(fp._1))
      .map(_._2)
      .foreach(addDiscardOrDominate(localSkylines, _))

    localSkylines
  }

  private def passesPreComparison(flag: Flag): Boolean = {
    for (i <- 0 until flag.size) {
      if (flag.getValueOf(i)) {
        return true
      }
    }

    false
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

  def computeSkylinesWithoutPreComparison(pointIterable: Iterable[Point]) = {
    val localSkylines = ListBuffer[Point]()
    for (candidateSkyline <- pointIterable) {
      addDiscardOrDominate(localSkylines, candidateSkyline)
    }

    localSkylines
  }
}
