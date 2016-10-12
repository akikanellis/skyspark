package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.test_utils.{SparkAddOn, UnitSpec}
import org.mockito.Mockito._

class FlagAdderTest extends UnitSpec with SparkAddOn {
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
    val expectedFlagPointPairs = Seq[(Flag, Point)](
      (Flag(true, false), Point(5.4, 4.4)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, true), Point(3.6, 9.0))
    )
    val pointsSeq = expectedFlagPointPairs.map(_._2)
    val points = sc.parallelize(pointsSeq)
    whenFlagIsRequestedForPointReturnIt(expectedFlagPointPairs)
    when(medianFinder.getMedian(points)).thenReturn(Point(2.7, 4.5))

    val actualFlagPointPairs = flagAdder.addFlags(points).collect

    actualFlagPointPairs should contain theSameElementsAs expectedFlagPointPairs
  }

  def whenFlagIsRequestedForPointReturnIt(flagPoints: Seq[(Flag, Point)]) = {
    flagPoints.foreach { case (f, p) => when(flagProducer.calculateFlag(p)).thenReturn(f) }
  }
}
