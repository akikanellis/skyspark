package com.akikanellis.skyspark.scala.api.algorithms

/**
  * An algorithm for determining if a point dominates another.
  */
private[algorithms] object DominatingAlgorithm {

  /**
    * Given two data points in a d-dimensional dataset, say p and q, we say p dominates q if for every i &isin; [1, d]
    * we have pi &le; qi and there exists at least one j such that pj &lt; qj .
    *
    * @param p The candidate point to check if it dominates the other point
    * @param q The other point to check if it is dominated by the candidate point
    * @return True if p dominates q, false if not
    */
  private[algorithms] def dominates(p: Point, q: Point): Boolean = {
    var atLeastOneDimensionIsSmaller = false

    for (i <- 0 until p.size) {
      val candidateDimension = p.dimension(i)
      val otherDimension = q.dimension(i)

      if (candidateDimensionIsWorse(candidateDimension, otherDimension)) return false

      if (candidateDimensionIsBetter(candidateDimension, otherDimension)) atLeastOneDimensionIsSmaller = true
    }

    atLeastOneDimensionIsSmaller
  }

  private def candidateDimensionIsWorse(candidateDimension: Double, otherDimension: Double): Boolean =
    candidateDimension > otherDimension

  private def candidateDimensionIsBetter(candidateDimension: Double, otherDimension: Double): Boolean =
    candidateDimension < otherDimension
}
