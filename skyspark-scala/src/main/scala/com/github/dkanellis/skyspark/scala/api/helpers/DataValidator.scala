package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import org.apache.spark.rdd.RDD

/**
  * Helper class for determining if the point RDD is valid or not.
  */
object DataValidator {

  /**
    * Validates the point RDD. If an invalid RDD was passed, an exception is thrown otherwise nothing happens.
    * <p>
    * An invalid point RDD will have points with different dimensions, for example Point(1, 2, 3) and Point(1, 2)
    * have a different number of dimensions.
    *
    * @param points The point RDD to examine
    * @throws InvalidDataException if there are different number of dimensions present
    */
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
