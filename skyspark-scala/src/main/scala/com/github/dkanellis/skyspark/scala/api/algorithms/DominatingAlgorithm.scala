package com.github.dkanellis.skyspark.scala.api.algorithms

private[algorithms] object DominatingAlgorithm {

  private[algorithms] def dominates(candidate: Point, other: Point): Boolean = {
    var atLeastOneSmaller = false

    for (i <- 0 until candidate.size) {
      val candidateDimension = candidate.dimension(i)
      val otherDimension = other.dimension(i)

      if (candidateDimensionIsWorse(candidateDimension, otherDimension)) return false

      if (candidateDimensionIsBetter(candidateDimension, otherDimension)) atLeastOneSmaller = true
    }

    atLeastOneSmaller
  }

  private def candidateDimensionIsWorse(candidateDimension: Double, otherDimension: Double): Boolean =
    candidateDimension > otherDimension

  private def candidateDimensionIsBetter(candidateDimension: Double, otherDimension: Double): Boolean =
    candidateDimension < otherDimension
}
