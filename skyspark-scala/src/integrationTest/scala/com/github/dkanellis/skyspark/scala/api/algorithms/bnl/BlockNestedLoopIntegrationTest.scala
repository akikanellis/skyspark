package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.helpers.TextFileToPointRdd
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class BlockNestedLoopIntegrationTest extends FlatSpec with BeforeAndAfter with Matchers with TableDrivenPropertyChecks
  with SparkAddOn {

  val pointFiles =
    Table(
      ("pointsFile", "skylinesFile"),
      ("/CORREL_2_10000.txt", "/CORREL_2_10000_SKYLINES.txt"),
      ("/UNIFORM_2_10000.txt", "/UNIFORM_2_10000_SKYLINES.txt"),
      ("/ANTICOR_2_10000.txt", "/ANTICOR_2_10000_SKYLINES.txt")
    )

  withSpark { sc =>
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