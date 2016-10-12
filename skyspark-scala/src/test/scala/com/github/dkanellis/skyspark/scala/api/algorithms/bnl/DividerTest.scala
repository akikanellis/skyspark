package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class DividerTest extends FlatSpec with BeforeAndAfter with Matchers with MockitoSugar with SparkAddOn {
  private var flagAdder: FlagAdder = _
  private var skylineComputer: SkylineComputer = _
  private var divider: Divider = _

  before {
    flagAdder = mock[FlagAdder]
    skylineComputer = mock[SkylineComputer]
    divider = new Divider(flagAdder, skylineComputer)
  }

  "A set of points" should "return the local skylines with their flags" in withSpark { sc =>
    val flagPointsSeq = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, true), Point(6.9, 5.6)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, false), Point(5.9, 4.0)))
    val pointsSeq = flagPointsSeq.map(_._2)
    val points = sc.parallelize(pointsSeq)
    val flagPoints = sc.parallelize(flagPointsSeq)
    when(flagAdder.addFlags(points)).thenReturn(flagPoints)
    val expectedLocalSkylinesWithFlagsSeq = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, false), Point(5.9, 4.0)))
    val expectedLocalSkylinesWithFlags = sc.parallelize(expectedLocalSkylinesWithFlagsSeq)
    when(skylineComputer.computeLocalSkylines(flagPoints)).thenReturn(expectedLocalSkylinesWithFlags)

    val actualLocalSkylinesWithFlags = divider.divide(points).collect

    actualLocalSkylinesWithFlags should contain theSameElementsAs expectedLocalSkylinesWithFlagsSeq
  }
}
