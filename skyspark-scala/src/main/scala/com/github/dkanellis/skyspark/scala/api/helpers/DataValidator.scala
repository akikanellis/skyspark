package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

object DataValidator {

  @throws(classOf[InvalidDataException])
  def validate(points: RDD[Point]) {
    val numberOfDifferentDimensions = points.map(_.size())
      .distinct()
      .count()

    if (numberOfDifferentDimensions != 1) {
      throw new InvalidDataException
    }
  }

  class InvalidDataException extends Exception {
    override def getMessage: String = "The data is invalid."
  }

}
