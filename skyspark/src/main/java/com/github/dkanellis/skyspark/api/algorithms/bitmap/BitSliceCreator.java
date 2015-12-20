package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import scala.Tuple2;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

interface BitSliceCreator extends Serializable {

    Tuple2<Long, BitSet> DEFAULT = new Tuple2<>(-1L, new BitSet(0));

    Tuple2<Long, BitSet> from(Tuple2<List<BitSet>, Tuple2<Double, Long>> data, Long sizeOfUniqueValues);
}
