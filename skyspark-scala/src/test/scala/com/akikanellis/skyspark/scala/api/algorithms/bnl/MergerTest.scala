package com.akikanellis.skyspark.scala.api.algorithms.bnl

import com.akikanellis.skyspark.scala.api.algorithms.Point
import com.akikanellis.skyspark.scala.test_utils.{SparkAddOn, UnitSpec}
import org.mockito.Mockito.when

class MergerTest extends UnitSpec with SparkAddOn {
  var bnlAlgorithm: BnlAlgorithm = _
  var merger: Merger = _

  before {
    bnlAlgorithm = mockForSpark[BnlAlgorithm]
    merger = new Merger(bnlAlgorithm)
  }

  "A set of flag-points" should "keep only the skylines and be merged" in withSpark { sc =>
    val flagsWithPointsSeq = Seq(
      (Flag(true, true), Point(5.9, 4.6)), (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, false), Point(5.9, 4.0)), (Flag(true, false), Point(6.7, 3.3)),
      (Flag(true, false), Point(6.1, 3.4)), (Flag(false, true), Point(2.5, 7.3)))
    val flagsWithPoints = sc.parallelize(flagsWithPointsSeq)
    val expectedSkylines = Seq(Point(5.0, 4.1), Point(5.9, 4.0), Point(2.5, 7.3), Point(6.7, 3.3), Point(6.1, 3.4))
    when(bnlAlgorithm.computeSkylinesWithPreComparison(flagsWithPointsSeq)).thenReturn(expectedSkylines)

    val actualSkylines = merger.merge(flagsWithPoints).collect

    actualSkylines should contain theSameElementsAs expectedSkylines
  }
}
