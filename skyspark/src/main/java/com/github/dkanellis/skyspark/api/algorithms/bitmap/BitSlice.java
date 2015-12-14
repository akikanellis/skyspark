package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.BitSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class BitSlice implements Serializable {

    private final long index;
    private final double dimensionValue;
    private final BitSet bitVector;

    public BitSlice(final long index, final double dimensionValue, @NotNull BitSet bitVector) {
        Preconditions.checkArgument(index >= 0, "Index can not be negative");
        this.index = index;
        this.dimensionValue = dimensionValue;
        this.bitVector = checkNotNull(bitVector);
    }

    public long getIndex() {
        return index;
    }

    public double getDimensionValue() {
        return dimensionValue;
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
                Double.compare(bitSlice.dimensionValue, dimensionValue) == 0 &&
                Objects.equal(bitVector, bitSlice.bitVector);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(index, dimensionValue, bitVector);
    }

    @Override
    public String toString() {
        return String.format("[%d, %.2f, %s]", index, dimensionValue, bitVector);
    }
}
