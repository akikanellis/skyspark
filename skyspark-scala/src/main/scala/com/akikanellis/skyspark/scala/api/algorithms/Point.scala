package com.akikanellis.skyspark.scala.api.algorithms

/**
  * Defines a point representing a location in a coordinate space whose size is defined by the number of dimensions
  * provided.
  */
case class Point(dimensions: Double*) extends Serializable {

  /**
    * The number of dimensions in this point.
    * <p>
    * For example the 2-dimensional point <i>Point(2.5, 5.3)</i> will return 2.
    * <p>
    * The 3-dimensional point <i>Point(6.7, 2.5, 5.3)</i> will return 3.
    *
    * @return The number of dimensions in this point
    */
  def size = dimensions.length

  /**
    * The value of a point's dimension.
    * <p>
    * For example:
    * {{{
    * val point = Point(2.5, 5.3)
    * print(point.dimension(0)) // Prints 2.5
    * }}}
    *
    * @param i The index of the dimension
    * @return The value of the dimension
    */
  def dimension(i: Int) = dimensions(i)

  /**
    * Checks if this point dominates another one by using the specified dominating algorithm.
    *
    * @param other The point to check against
    * @return True if this point dominates the other, false if not
    */
  def dominates(other: Point): Boolean = DominatingAlgorithm.dominates(this, other)
}
