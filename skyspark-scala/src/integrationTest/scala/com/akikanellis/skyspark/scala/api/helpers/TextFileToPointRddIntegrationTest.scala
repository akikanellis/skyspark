package com.akikanellis.skyspark.scala.api.helpers

import com.akikanellis.skyspark.scala.api.algorithms.Point
import com.akikanellis.skyspark.scala.test_utils.IntegrationSpec
import org.apache.hadoop.mapred.InvalidInputException

class TextFileToPointRddIntegrationTest extends IntegrationSpec {

  "A file with points" should "convert correctly" in withSpark { sc =>
    val pointsFilePath = getClass.getResource("/ANTICOR_2_10.txt").getFile
    val expectedPoints = Seq(
      Point(5.4, 4.4), Point(5.0, 4.1), Point(3.6, 9.0), Point(5.9, 4.0), Point(5.9, 4.6), Point(2.5, 7.3),
      Point(6.3, 3.5), Point(9.9, 4.1), Point(6.7, 3.3), Point(6.1, 3.4))

    val actualPoints = TextFileToPointRdd.convert(sc, pointsFilePath, " ").collect

    actualPoints should contain theSameElementsAs expectedPoints
  }

  "A missing file" should "throw an InvalidInputException" in withSpark { sc =>
    val pointsFilePath = "/this_is_not_an_existing_file.txt"

    an[InvalidInputException] should be thrownBy TextFileToPointRdd.convert(sc, pointsFilePath, " ").collect
  }
}
