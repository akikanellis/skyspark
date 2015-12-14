package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import scala.Tuple2;

import java.io.Serializable;
import java.util.BitSet;

interface BitSliceCreator extends Serializable {
    BitSlice from(Tuple2<Tuple2<Double, Long>, Iterable<BitSet>> data);
}
