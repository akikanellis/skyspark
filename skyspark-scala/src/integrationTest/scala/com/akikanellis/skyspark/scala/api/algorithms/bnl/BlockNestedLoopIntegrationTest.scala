package com.akikanellis.skyspark.scala.api.algorithms.bnl

import com.akikanellis.skyspark.scala.api.helpers.TextFileToPointRdd
import com.akikanellis.skyspark.scala.test_utils.IntegrationSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class BlockNestedLoopIntegrationTest extends IntegrationSpec with TableDrivenPropertyChecks {

  val pointFiles =
    Table(
      ("pointsFile", "skylinesFile"),
      ("/CORREL_2_10000.txt", "/CORREL_2_10000_SKYLINES.txt"),
      ("/UNIFORM_2_10000.txt", "/UNIFORM_2_10000_SKYLINES.txt"),
      ("/ANTICOR_2_10000.txt", "/ANTICOR_2_10000_SKYLINES.txt")
    )

  "All point types" should "produce correct skylines" in withSpark { sc =>
    forAll(pointFiles) { (pointsFile: String, skylinesFile: String) =>

      val pointsFilePath = getClass.getResource(pointsFile).getFile
      val expectedSkylinesFilePath = getClass.getResource(skylinesFile).getFile
      val points = TextFileToPointRdd.convert(sc, pointsFilePath, " ")
      val expectedSkylines = TextFileToPointRdd.convert(sc, expectedSkylinesFilePath, " ")

      val bnl = new BlockNestedLoop

      val actualSkylines = bnl.computeSkylinePoints(points)

      actualSkylines.collect() should contain theSameElementsAs expectedSkylines.collect()
    }
  }
}
