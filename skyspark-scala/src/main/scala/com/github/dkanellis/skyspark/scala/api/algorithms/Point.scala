package com.github.dkanellis.skyspark.scala.api.algorithms

case class Point(private val dimensions: Double*) extends Serializable {

  def size() = dimensions.length

  def dimension(i: Int) = dimensions(i)
}
