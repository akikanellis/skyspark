package com.akikanellis.skyspark.scala.api.helpers

import com.akikanellis.skyspark.scala.api.algorithms.Point
import com.akikanellis.skyspark.scala.test_utils.IntegrationSpec
import org.apache.hadoop.mapred.InvalidInputException

class TextFileToPointRddIntegrationTest extends IntegrationSpec {

  "A file with points" should "convert correctly" in withSpark {sc =>
    val pointsFilePath = getClass.getResource("/UNIFORM_2_10.txt").getFile
    val expectedPoints = Seq(
      Point(7.4, 6.4), Point(5.2, 1.0), Point(4.3, 8.9), Point(6.7, 3.8), Point(7.3, 2.5), Point(7.6, 9.3),
      Point(4.0, 1.5), Point(1.7, 5.6), Point(6.4, 3.7), Point(2.9, 8.4))

    val actualPoints = TextFileToPointRdd.convert(sc, pointsFilePath, " ").collect

    actualPoints should contain theSameElementsAs expectedPoints
  }

  "A missing file" should "throw an InvalidInputException" in withSpark {sc =>
    val pointsFilePath = "/this_is_not_an_existing_file.txt"

    an[InvalidInputException] should be thrownBy TextFileToPointRdd.convert(sc, pointsFilePath, " ").collect
  }
}
