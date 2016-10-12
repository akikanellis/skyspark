package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class DividerTest extends FlatSpec with BeforeAndAfter with Matchers with MockitoSugar with SparkAddOn {
  private var medianFinder: MedianFinder = _
  private var bnlAlgorithm: BnlAlgorithm = _
  private var divider: Divider = _

  before {
    medianFinder = mock[MedianFinder]
    bnlAlgorithm = mock[BnlAlgorithm]
    divider = new Divider(medianFinder, bnlAlgorithm)
  }

  "A set of points" should "return the local skylines with their flags" in withSpark { sc =>
    val pointsSeq = Seq(
      Point(5.4, 4.4), Point(5.0, 4.1), Point(3.6, 9.0), Point(5.9, 4.0),
      Point(5.9, 4.6), Point(2.5, 7.3), Point(6.3, 3.5), Point(9.9, 4.1),
      Point(6.7, 3.3), Point(6.1, 3.4))
    val points = sc.parallelize(pointsSeq)
    when(medianFinder.getMedian(points)).thenReturn(Point(9.9 / 2, 9.0 / 2))
    when(bnlAlgorithm.computeSkylinesWithoutPreComparison()).thenReturn(Point(9.9 / 2, 9.0 / 2))
    val expectedSkylinesWithFlags = Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, false), Point(5.0, 4.1)), (Flag(true, false), Point(5.9, 4.0)),
      (Flag(true, false), Point(6.7, 3.3)), (Flag(true, false), Point(6.1, 3.4)),
      (Flag(false, true), Point(2.5, 7.3)))


    val actualSkylinesWithFlags = divider.divide(points)

    actualSkylinesWithFlags.collect() should contain theSameElementsAs expectedSkylinesWithFlags
  }
}
