package com.github.dkanellis.skyspark.scala.api.algorithms.bnl

case class Flag(bits: Boolean*) {

  def size = bits.length

  def bit(i: Int) = bits(i)
}
