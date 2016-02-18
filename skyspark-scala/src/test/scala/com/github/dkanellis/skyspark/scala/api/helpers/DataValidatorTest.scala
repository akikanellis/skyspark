package com.github.dkanellis.skyspark.scala.api.helpers

import com.github.dkanellis.skyspark.scala.api.algorithms.Point
import com.github.dkanellis.skyspark.scala.api.helpers.DataValidator.InvalidDataException
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class DataValidatorTest extends FlatSpec with BeforeAndAfter {

  private var sc: SparkContext = _

  before {
    val sparkConf = new SparkConf().setAppName("DataValidator tests").setMaster("local[*]")
    sc = new SparkContext(sparkConf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "Points with different dimensions" should "throw InvalidDataException" in {
    val pointsArray = Seq[Point](new Point(1, 1), new Point(3, 2, 1))
    val points = sc.parallelize(pointsArray)

    intercept[InvalidDataException] {
      DataValidator.validate(points)
    }
  }

  "Points with same dimensions" should "not throw InvalidDataException" in {
    val pointsArray = Seq[Point](new Point(1, 1), new Point(3, 2))
    val points = sc.parallelize(pointsArray)
    DataValidator.validate(points)
  }
}
