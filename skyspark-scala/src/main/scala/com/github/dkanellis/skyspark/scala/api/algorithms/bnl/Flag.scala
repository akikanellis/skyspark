package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

/**
  * A representation of a bit-flag using booleans.
  *
  * @param bits Each position represents a bit in the bit-flag
  */
private[bnl] case class Flag(private[bnl] val bits: Boolean*) extends Serializable {

  private[bnl] def size = bits.length

  private[bnl] def bit(i: Int) = bits(i)
}
