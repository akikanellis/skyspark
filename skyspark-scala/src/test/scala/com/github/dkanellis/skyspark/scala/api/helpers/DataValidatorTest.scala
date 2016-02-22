package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.SparkAddOn
import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.api.helpers.DataValidator.InvalidDataException
import org.scalatest.{FlatSpec, Matchers}

class DataValidatorTest extends FlatSpec with Matchers with SparkAddOn {

  "Points with different dimensions" should "throw InvalidDataException" in withSpark { sc =>
    val pointsArray = Seq(Point(1, 1), Point(3, 2, 1))
    val points = sc.parallelize(pointsArray)

    an[InvalidDataException] should be thrownBy DataValidator.validate(points)
  }

  "Points with same dimensions" should "not throw InvalidDataException" in withSpark { sc =>
    val pointsArray = Seq(Point(1, 1), Point(3, 2))
    val points = sc.parallelize(pointsArray)

    noException should be thrownBy DataValidator.validate(points)
  }
}
