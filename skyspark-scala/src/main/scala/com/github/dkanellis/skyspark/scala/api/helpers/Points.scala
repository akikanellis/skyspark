package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point

object Points {

  def dominates(first: Point, second: Point): Boolean = {
    var atLeastOneSmaller = false
    for (i <- 0 until first.size()) {
      if (first.dimension(i) > second.dimension(i)) {
        return false
      }

      if (first.dimension(i) < second.dimension(i)) {
        atLeastOneSmaller = true
      }
    }

    atLeastOneSmaller
  }
}
