package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.api.helpers.DataValidator.InvalidDataException
import com.github.dkanellis.skyspark.scala.test_utils.{SparkAddOn, UnitSpec}
import org.apache.spark.rdd.RDD

class DataValidatorTest extends UnitSpec with SparkAddOn {

  "Points with different dimensions" should "throw InvalidDataException" in withSpark { sc =>
    val points: RDD[Point] = sc.parallelize(Seq(Point(1, 1), Point(3, 2, 1)))

    an[InvalidDataException] should be thrownBy DataValidator.validate(points)
  }

  "Points with the same dimensions" should "not throw InvalidDataException" in withSpark { sc =>
    val pointsArray = Seq(Point(1, 1), Point(3, 2))
    val points = sc.parallelize(pointsArray)

    noException should be thrownBy DataValidator.validate(points)
  }
}
