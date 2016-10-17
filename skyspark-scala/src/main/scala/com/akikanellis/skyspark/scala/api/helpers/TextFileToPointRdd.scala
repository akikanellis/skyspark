package com.akikanellis.skyspark.scala.api.helpers

import com.akikanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Helper for converting a text file to a point RDD. The file should have one point per line.
  */
object TextFileToPointRdd {

  /**
    * Converts the text file to a point RDD.
    *
    * @param sparkContext An active Spark context
    * @param filePath     The full path to the file
    * @param delimiter    Any single character except a period
    * @return The constructed point RDD
    */
  def convert(sparkContext: SparkContext, filePath: String, delimiter: String = " "): RDD[Point] = {
    sparkContext.textFile(filePath)
      .map(pointFromTextLine(_, delimiter))
  }

  private[helpers] def pointFromTextLine(textLine: String, delimiter: String) = {
    val numberArray = textLine
      .trim
      .split(delimiter)
      .map(_.toDouble)
    Point(numberArray: _*)
  }
}
