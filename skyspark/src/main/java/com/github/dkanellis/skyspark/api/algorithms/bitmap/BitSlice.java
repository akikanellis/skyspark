package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.BitSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class BitSlice implements Serializable {

    private final long index;
    private final double value;
    private final BitSet bitVector;

    public BitSlice(final long index, final double value, @NotNull BitSet bitVector) {
        Preconditions.checkArgument(index >= 0, "Index can not be negative");
        this.index = index;
        this.value = value;
        this.bitVector = checkNotNull(bitVector);
    }

    public static BitSlice fromTuple(@NotNull Tuple2<Tuple2<Double, Long>, Iterable<BitSet>> tuple) {
        final long index = tuple._1()._2();
        final double value = tuple._1()._1();
        final BitSet bitVector = sliceBits(tuple._2(), index);

        return new BitSlice(index, value, bitVector);
    }

    private static BitSet sliceBits(Iterable<BitSet> bitSets, Long sliceAt) {
        BitSet bitSlice = new BitSet();
        final int numberOfElements = Iterables.size(bitSets);
        final int lastIndex = numberOfElements - 1;
        int i = 0;
        for (BitSet bitVector : bitSets) {
            bitSlice.set(lastIndex - i++, bitVector.get(sliceAt.intValue()));
        }

        return bitSlice;
    }

    public long getIndex() {
        return index;
    }

    public double getValue() {
        return value;
    }

    public BitSet getBitVector() {
        return bitVector;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BitSlice bitSlice = (BitSlice) o;
        return index == bitSlice.index &&
                Double.compare(bitSlice.value, value) == 0 &&
                Objects.equal(bitVector, bitSlice.bitVector);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(index, value, bitVector);
    }

    @Override
    public String toString() {
        return String.format("[%d, %.2f, %s]", index, value, bitVector);
    }
}
