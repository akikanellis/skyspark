package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

import com.google.common.base.Preconditions

class Flag(bitsC: Boolean*) extends Serializable {

  val bits = bitsC

  def getValueOf(dimension: Int) = {
    Preconditions.checkElementIndex(dimension, bits.length)
    bits(dimension)
  }

  override def equals(other: Any): Boolean = other match {
    case that: Flag =>
      (that canEqual this) &&
        bits == that.bits
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Flag]

  override def hashCode(): Int = {
    val state = Seq(bits)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Flag($bits)"
}
