package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.google.common.base.Objects;
import scala.Tuple2;

import java.util.BitSet;

class BitSlices {

    private final BitSet firstSlice;
    private final BitSet secondSlice;

    BitSlices(BitSet firstSlice, BitSet secondSlice) {
        this.firstSlice = firstSlice;
        this.secondSlice = secondSlice;
    }

    static BitSlices fromTuple(Tuple2<BitSet, BitSet> tuple) {
        return new BitSlices(tuple._1(), tuple._2());
    }

    BitSet getFirstSlice() {
        return firstSlice;
    }

    BitSet getSecondSlice() {
        return secondSlice;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BitSlices bitSlices = (BitSlices) o;
        return Objects.equal(firstSlice, bitSlices.firstSlice) &&
                Objects.equal(secondSlice, bitSlices.secondSlice);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(firstSlice, secondSlice);
    }
}
