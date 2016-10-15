package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.test_utils.{SparkAddOn, UnitSpec}
import org.mockito.Mockito.when

class BlockNestedLoopTest extends UnitSpec with SparkAddOn {
  var divider: Divider = _
  var merger: Merger = _
  var blockNestedLoop: BlockNestedLoop = _

  before {
    divider = mock[Divider]
    merger = mock[Merger]
    blockNestedLoop = new BlockNestedLoop(divider, merger)
  }

  "An RDD with no points" should "throw IllegalArgumentException" in withSpark { sc =>
    val points = sc.parallelize(Seq.empty[Point])

    an[IllegalArgumentException] should be thrownBy blockNestedLoop.computeSkylinePoints(points)
  }

  "An RDD with points" should "return the skylines" in withSpark { sc =>
    val points = sc.parallelize(Seq(Point(5.9, 4.6), Point(5.0, 4.1), Point(3.6, 9.0)))
    val expectedSkylines = sc.parallelize(Seq(Point(5.0, 4.1), Point(3.6, 9.0)))
    val localSkylinesWithFlags = sc.parallelize(Seq(
      (Flag(true, true), Point(5.9, 4.6)),
      (Flag(true, false), Point(5.0, 4.1)),
      (Flag(true, true), Point(3.6, 9.0))))
    when(divider.divide(points)).thenReturn(localSkylinesWithFlags)
    when(merger.merge(localSkylinesWithFlags)).thenReturn(expectedSkylines)

    val actualSkylines = blockNestedLoop.computeSkylinePoints(points)

    actualSkylines.collect should contain theSameElementsAs expectedSkylines.collect
  }
}
