package com.akikanellis.skyspark.scala.api.algorithms.bnl

import com.akikanellis.skyspark.data.DatasetFiles
import com.akikanellis.skyspark.scala.api.helpers.TextFileToPointRdd
import com.akikanellis.skyspark.scala.test_utils.IntegrationSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class BlockNestedLoopIntegrationTest extends IntegrationSpec with TableDrivenPropertyChecks {

  val pointFiles =
    Table(
      "Dataset",
      DatasetFiles.CORREL_2_10000,
      DatasetFiles.UNIFORM_2_10000,
      DatasetFiles.ANTICOR_2_10000
    )

  "All point types" should "produce correct skylines" in withSpark { sc =>
    forAll(pointFiles) { (dataset: DatasetFiles) =>

      val pointsFilePath = dataset.pointsPath
      val expectedSkylinesFilePath = dataset.skylinesPath
      val points = TextFileToPointRdd.convert(sc, pointsFilePath, " ")
      val expectedSkylines = TextFileToPointRdd.convert(sc, expectedSkylinesFilePath, " ")

      val bnl = new BlockNestedLoop

      val actualSkylines = bnl.computeSkylinePoints(points)

      actualSkylines.collect() should contain theSameElementsAs expectedSkylines.collect()
    }
  }
}
