package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.google.common.collect.Iterables;
import scala.Tuple2;

import java.util.BitSet;

public class BitSliceCreatorImpl implements BitSliceCreator {

    @Override
    public BitSlice from(Tuple2<Tuple2<Double, Long>, Iterable<BitSet>> data) {
        final long index = data._1()._2();
        final double dimensionValue = data._1()._1();
        final BitSet bitVector = sliceBits(data._2(), index);

        return new BitSlice(index, dimensionValue, bitVector);
    }

    private BitSet sliceBits(Iterable<BitSet> bitSets, Long sliceAt) {
        BitSet bitSlice = new BitSet();
        final int numberOfElements = Iterables.size(bitSets);
        final int lastIndex = numberOfElements - 1;
        int i = 0;
        for (BitSet bitVector : bitSets) {
            bitSlice.set(lastIndex - i++, bitVector.get(sliceAt.intValue()));
        }

        return bitSlice;
    }
}
