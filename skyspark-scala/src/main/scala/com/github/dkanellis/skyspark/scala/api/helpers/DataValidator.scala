package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

object DataValidator {

  @throws(classOf[InvalidDataException]) def validate(points: RDD[Point]) {
    val numberOfDifferentDimensions = points.map(_.size())
      .distinct
      .count

    if (numberOfDifferentDimensions != 1) throw new InvalidDataException(numberOfDifferentDimensions)
  }

  class InvalidDataException(numberOfDimensions: Long) extends Exception {
    override def getMessage: String =
      s"The data has points with differing dimensions. Total dimensions found: $numberOfDimensions"
  }

}
