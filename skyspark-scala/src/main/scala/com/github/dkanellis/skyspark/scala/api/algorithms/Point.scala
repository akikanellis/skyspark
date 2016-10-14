package com.github.dkanellis.skyspark.scala.api.algorithms

/**
  * Defines a point representing a location in a coordinate space whose size is defined by the number of dimensions
  * provided.
  */
case class Point(private val dimensions: Double*) extends Serializable {

  def size() = dimensions.length

  def dimension(i: Int) = dimensions(i)

  /**
    * Checks if this point dominates another one by using the specified dominating algorithm.
    *
    * @param other The point to check against
    * @return True if this point dominates the other, false if not
    */
  def dominates(other: Point): Boolean = DominatingAlgorithm.dominates(this, other)
}
