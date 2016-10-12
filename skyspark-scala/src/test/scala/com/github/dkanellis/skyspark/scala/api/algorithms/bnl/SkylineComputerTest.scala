package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.test_utils.UnitSpec
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.runners.MockitoJUnitRunner

@RunWith(classOf[MockitoJUnitRunner])
class SkylineComputerTest extends UnitSpec with SparkAddOn {
  var bnlAlgorithm: BnlAlgorithm = _
  var skylineComputer: SkylineComputer = _

  before {
    bnlAlgorithm = mock[BnlAlgorithm](withSettings().serializable())
    skylineComputer = new SkylineComputer(bnlAlgorithm)
  }

  "A set of points with their flags" should "produce the set of local skylines per key" in withSpark { sc =>
    val flagPointsSeq = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, true), Point(6.9, 5.6)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, false), Point(5.9, 4.0)))
    val flagPointsWithExpectedLocalSkylines = Seq(
      (Seq(Point(5.9, 4.6), Point(6.9, 5.6)), Seq(Point(5.9, 4.6))),
      (Seq(Point(5.0, 4.1), Point(5.9, 4.0)), Seq(Point(5.0, 4.1), Point(5.9, 4.0))))
    whenAPointGroupIsToBeComputedReturnSkylines(flagPointsWithExpectedLocalSkylines)
    val flagPoints = sc.parallelize(flagPointsSeq)
    val expectedLocalSkylinesWithFlags = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, false), Point(5.9, 4.0)))

    val localSkylinesWithFlags = skylineComputer.computeLocalSkylines(flagPoints).collect

    localSkylinesWithFlags should contain theSameElementsAs expectedLocalSkylinesWithFlags
  }

  private def whenAPointGroupIsToBeComputedReturnSkylines
  (flagPointsWithExpectedLocalSkylines: Seq[(Seq[Point], Seq[Point])]) = {
    flagPointsWithExpectedLocalSkylines.foreach {
      case (fp, exp) => when(bnlAlgorithm.computeSkylinesWithoutPreComparison(fp)).thenReturn(exp)
    }
  }
}
