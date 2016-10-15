package com.akikanellis.skyspark.scala.api.algorithms.bnl

import com.akikanellis.skyspark.scala.api.algorithms.Point
import com.akikanellis.skyspark.scala.test_utils.{SparkAddOn, UnitSpec}
import org.mockito.Mockito._

class LocalSkylineCalculatorTest extends UnitSpec with SparkAddOn {
  var bnlAlgorithm: BnlAlgorithm = _
  var localSkylineCalculator: LocalSkylineCalculator = _

  before {
    bnlAlgorithm = mockForSpark[BnlAlgorithm]
    localSkylineCalculator = new LocalSkylineCalculator(bnlAlgorithm)
  }

  "A set of points with their flags" should "produce the set of local skylines per key" in withSpark { sc =>
    val flagPointsSeq = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, true), Point(6.9, 5.6)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, false), Point(5.9, 4.0)))
    val pointsWithExpectedLocalSkylines = Seq(
      (Seq(Point(5.9, 4.6), Point(6.9, 5.6)), Seq(Point(5.9, 4.6))),
      (Seq(Point(5.0, 4.1), Point(5.9, 4.0)), Seq(Point(5.0, 4.1), Point(5.9, 4.0))))
    whenAPointGroupIsToBeComputedReturnSkylines(pointsWithExpectedLocalSkylines)
    val flagPoints = sc.parallelize(flagPointsSeq)
    val expectedLocalSkylinesWithFlags = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, false), Point(5.9, 4.0)))

    val localSkylinesWithFlags = localSkylineCalculator.computeLocalSkylines(flagPoints).collect

    localSkylinesWithFlags should contain theSameElementsAs expectedLocalSkylinesWithFlags
  }

  private def whenAPointGroupIsToBeComputedReturnSkylines(pointsWithExpectedLocalSkylines: Seq[(Seq[Point], Seq[Point])]) = {
    pointsWithExpectedLocalSkylines.foreach {
      case (points, skylines) => when(bnlAlgorithm.computeSkylinesWithoutPreComparison(points)).thenReturn(skylines)
    }
  }
}
