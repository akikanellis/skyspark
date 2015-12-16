package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.google.common.collect.Iterables;
import scala.Tuple2;

import java.util.BitSet;
import java.util.List;

public class BitSliceCreatorImpl implements BitSliceCreator {

    @Override
    public Tuple2<Long, BitSlice> defaultValue() {
        return new Tuple2<>(-1L, new BitSlice(-1L, 0, new BitSet(0)));
    }

    @Override
    public Tuple2<Long, BitSlice> from(Tuple2<List<BitSet>, Tuple2<Double, Long>> data, Long sizeOfUniqueValues) {
        final long index = data._2()._2();
        final double dimensionValue = data._2()._1();
        final BitSet bitVector = sliceBits(data._1(), sizeOfUniqueValues - index - 1);

        return new Tuple2<>(index, new BitSlice(index, dimensionValue, bitVector));
    }

    private BitSet sliceBits(Iterable<BitSet> bitSets, Long sliceAt) {
        BitSet bitSlice = new BitSet();
        final int numberOfElements = Iterables.size(bitSets);
        int lastIndex = numberOfElements - 1;
        for (BitSet bitVector : bitSets) {
            bitSlice.set(lastIndex--, bitVector.get(sliceAt.intValue()));
        }

        return bitSlice;
    }
}
