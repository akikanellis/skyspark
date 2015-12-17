package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.google.common.collect.Iterables;
import scala.Tuple2;

import java.util.BitSet;
import java.util.List;

public class BitSliceCreatorImpl implements BitSliceCreator {

    private static final Long INDEX_OFFSET = -1L;

    // TODO cache sizeOfUniqueValues
    @Override
    public Tuple2<Long, BitSet> from(Tuple2<List<BitSet>, Tuple2<Double, Long>> data, Long sizeOfUniqueValues) {
        final Iterable<BitSet> bitSetsOfAllElements = data._1();
        final Long index = data._2()._2();
        final BitSet bitSlice = sliceBits(bitSetsOfAllElements, calculateSlicePosition(sizeOfUniqueValues, index).intValue());

        return new Tuple2<>(index, bitSlice);
    }

    private Long calculateSlicePosition(Long sizeOfUniqueValues, Long index) {
        return sizeOfUniqueValues - index + INDEX_OFFSET;
    }

    private BitSet sliceBits(Iterable<BitSet> bitSets, Integer sliceAt) {
        BitSet bitSlice = new BitSet();
        final int numberOfElements = Iterables.size(bitSets); // TODO cache this as well
        int lastIndex = numberOfElements - 1;
        for (BitSet bitVector : bitSets) {
            bitSlice.set(lastIndex--, bitVector.get(sliceAt));
        }

        return bitSlice;
    }
}
