package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.test_utils.UnitSpec

class BnlAlgorithmTest extends UnitSpec {
  var bnlAlgorithm: BnlAlgorithm = _

  before { bnlAlgorithm = new BnlAlgorithm }

  "Without pre-comparison a set of points" should "compute the skylines" in {
    val points = Seq(
      Point(5.4, 4.4), Point(5.0, 4.1), Point(3.6, 9.0), Point(5.9, 4.0), Point(5.9, 4.6), Point(2.5, 7.3),
      Point(6.3, 3.5), Point(9.9, 4.1), Point(6.7, 3.3), Point(6.1, 3.4))
    val expectedSkylines = Seq(Point(5.0, 4.1), Point(5.9, 4.0), Point(2.5, 7.3), Point(6.7, 3.3), Point(6.1, 3.4))

    val actualSkylines = bnlAlgorithm.computeSkylinesWithoutPreComparison(points)

    actualSkylines should contain theSameElementsAs expectedSkylines
  }

  "Without pre-comparison a set of only skylines" should "have nothing changed" in {
    val points = Seq(Point(5.0, 4.1), Point(5.9, 4.0), Point(2.5, 7.3), Point(6.7, 3.3), Point(6.1, 3.4))

    val actualSkylines = bnlAlgorithm.computeSkylinesWithoutPreComparison(points)

    actualSkylines should contain theSameElementsAs points
  }

  "With pre-comparison a set of points with their flags" should "compute the skylines" in {
    val flagsPoints = Seq(
      (Flag(true, true), Point(5.9, 4.6)), (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, false), Point(5.9, 4.0)), (Flag(true, false), Point(6.7, 3.3)),
      (Flag(true, false), Point(6.1, 3.4)), (Flag(false, true), Point(2.5, 7.3)))
    val expectedSkylines = Seq(Point(5.0, 4.1), Point(5.9, 4.0), Point(2.5, 7.3), Point(6.7, 3.3), Point(6.1, 3.4))

    val actualSkylines = bnlAlgorithm.computeSkylinesWithPreComparison(flagsPoints)

    actualSkylines should contain theSameElementsAs expectedSkylines
  }

  "With pre-comparison a set of only skylines with their flags" should "have nothing changed" in {
    val flagsPoints = Seq(
      (Flag(true, false), Point(5.0, 4.1)), (Flag(true, false), Point(5.9, 4.0)), (Flag(true, false), Point(6.7, 3.3)),
      (Flag(true, false), Point(6.1, 3.4)), (Flag(false, true), Point(2.5, 7.3)))
    val expectedSkylines = Seq(Point(5.0, 4.1), Point(5.9, 4.0), Point(6.7, 3.3), Point(6.1, 3.4), Point(2.5, 7.3))

    val actualSkylines = bnlAlgorithm.computeSkylinesWithPreComparison(flagsPoints)

    actualSkylines should contain theSameElementsAs expectedSkylines
  }
}
