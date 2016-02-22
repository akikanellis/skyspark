package com.github.dkanellis.skyspark.scala.api.algorithms

case class Point(private val dimensions: Double*) {

  def size() = dimensions.length

  def dimension(i: Int) = dimensions(i)
}
