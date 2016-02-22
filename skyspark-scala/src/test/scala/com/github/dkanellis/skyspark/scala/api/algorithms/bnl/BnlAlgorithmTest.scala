package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.scalatest.{FlatSpec, Matchers}

class BnlAlgorithmTest extends FlatSpec with Matchers {

  "Without pre-comparisson a set of points" should "compute the skylines" in {
    val points = Seq(
      new Point(5.4, 4.4), new Point(5.0, 4.1), new Point(3.6, 9.0), new Point(5.9, 4.0),
      new Point(5.9, 4.6), new Point(2.5, 7.3), new Point(6.3, 3.5), new Point(9.9, 4.1),
      new Point(6.7, 3.3), new Point(6.1, 3.4))
    val expectedSkylines = Seq(
      new Point(5.0, 4.1), new Point(5.9, 4.0), new Point(2.5, 7.3), new Point(6.7, 3.3),
      new Point(6.1, 3.4))

    val actualSkylines = BnlAlgorithm.computeSkylinesWithoutPreComparison(points)

    actualSkylines should contain theSameElementsAs expectedSkylines
  }

  "Without pre-comparisson a set of only skylines" should "have nothing changed" in {
    val points = Seq(
      new Point(5.0, 4.1), new Point(5.9, 4.0), new Point(2.5, 7.3), new Point(6.7, 3.3),
      new Point(6.1, 3.4))

    val actualSkylines = BnlAlgorithm.computeSkylinesWithoutPreComparison(points)

    actualSkylines should contain theSameElementsAs points
  }

  "With pre-comparisson a set of points with their flags" should "compute the skylines" in {
    val flagsPoints = Seq(
      (new Flag(true, true), new Point(5.9, 4.6)),
      (new Flag(true, false), new Point(5.0, 4.1)), (new Flag(true, false), new Point(5.9, 4.0)),
      (new Flag(true, false), new Point(6.7, 3.3)), (new Flag(true, false), new Point(6.1, 3.4)),
      (new Flag(false, true), new Point(2.5, 7.3)))
    val expectedSkylines = Seq(
      new Point(5.0, 4.1), new Point(5.9, 4.0), new Point(2.5, 7.3), new Point(6.7, 3.3),
      new Point(6.1, 3.4))

    val actualSkylines = BnlAlgorithm.computeSkylinesWithPreComparison(flagsPoints)

    actualSkylines should contain theSameElementsAs expectedSkylines
  }

  "With pre-comparisson a set of only skylines with their flags" should "have nothing changed" in {
    val flagsPoints = Seq(
      (new Flag(true, false), new Point(5.0, 4.1)), (new Flag(true, false), new Point(5.9, 4.0)),
      (new Flag(true, false), new Point(6.7, 3.3)), (new Flag(true, false), new Point(6.1, 3.4)),
      (new Flag(false, true), new Point(2.5, 7.3)))
    val expectedSkylines = Seq(
      new Point(5.0, 4.1), new Point(5.9, 4.0),
      new Point(6.7, 3.3), new Point(6.1, 3.4),
      new Point(2.5, 7.3))

    val actualSkylines = BnlAlgorithm.computeSkylinesWithPreComparison(flagsPoints)

    actualSkylines should contain theSameElementsAs expectedSkylines
  }
}