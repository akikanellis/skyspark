package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import scala.Tuple2;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

interface BitSliceCreator extends Serializable {

    Tuple2<Long, BitSlice> defaultValue();

    Tuple2<Long, BitSlice> from(Tuple2<List<BitSet>, Tuple2<Double, Long>> data, Long sizeOfUniqueValues);
}
