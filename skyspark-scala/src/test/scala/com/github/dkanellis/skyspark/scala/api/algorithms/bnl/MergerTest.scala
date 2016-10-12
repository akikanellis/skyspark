package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.test_utils.{SparkAddOn, UnitSpec}

class MergerTest extends UnitSpec with SparkAddOn {

  private var merger: Merger = _

  before {
    merger = new Merger
  }

  "A set of flag-points" should "keep only the skylines and be merged" in withSpark { sc =>
    val flagPointsSeq = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, false), Point(5.0, 4.1)), (Flag(true, false), Point(5.9, 4.0)),
      (Flag(true, false), Point(6.7, 3.3)), (Flag(true, false), Point(6.1, 3.4)),
      (Flag(false, true), Point(2.5, 7.3)))
    val flagPoints = sc.parallelize(flagPointsSeq)
    val expectedSkylines = Seq(
      Point(5.0, 4.1), Point(5.9, 4.0), Point(2.5, 7.3), Point(6.7, 3.3),
      Point(6.1, 3.4))

    val actualSkylines = merger.merge(flagPoints)

    actualSkylines.collect() should contain theSameElementsAs expectedSkylines
  }
}
