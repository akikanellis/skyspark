package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.SparkContext

object TextFileToPointRdd {

  def convert(sparkContext: SparkContext, filePath: String, delimiter: String) = {
    sparkContext.textFile(filePath)
      .map(pointFromTextLine(_, delimiter))
  }

  private def pointFromTextLine(textLine: String, delimiter: String) = {
    new Point(
      textLine.trim
        .split(delimiter)
        .map(_.toDouble): _*)
  }
}
