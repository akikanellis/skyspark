package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class FlagAdderTest extends FlatSpec with BeforeAndAfter with Matchers with MockitoSugar with SparkAddOn {
  var medianFinder: MedianFinder = _
  var flagProducer: FlagProducer = _
  var flagAdder: FlagAdder = _

  before {
    medianFinder = mock[MedianFinder]
    flagProducer = mock[FlagProducer](withSettings().serializable())
    flagAdder = new FlagAdder(medianFinder) {
      override protected def createFlagProducer(median: Point): FlagProducer = flagProducer
    }
  }

  "A set of points" should "be returned with their respective flags" in withSpark { sc =>
    val pointsSeq = Seq(Point(5.4, 4.4), Point(5.0, 4.1), Point(3.6, 9.0))
    val points = sc.parallelize(pointsSeq)
    when(flagProducer.calculateFlag(Point(5.4, 4.4))).thenReturn(Flag(true, false))
    when(flagProducer.calculateFlag(Point(5.0, 4.1))).thenReturn(Flag(true, false))
    when(flagProducer.calculateFlag(Point(3.6, 9.0))).thenReturn(Flag(true, true))
    when(medianFinder.getMedian(points)).thenReturn(Point(2.7, 4.5))
    val expectedFlagPointPairs = Seq[(Flag, Point)](
      (Flag(true, false), Point(5.4, 4.4)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, true), Point(3.6, 9.0))
    )

    val actualFlagPointPairs = flagAdder.addFlags(points).collect

    actualFlagPointPairs should contain theSameElementsAs expectedFlagPointPairs
  }

  //  def private combine(points: Seq[Point])
}
