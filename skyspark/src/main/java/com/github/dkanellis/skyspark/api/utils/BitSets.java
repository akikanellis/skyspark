package com.github.dkanellis.skyspark.api.utils;

import java.util.BitSet;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;
import static com.google.common.base.Preconditions.checkArgument;

public final class BitSets {

    private BitSets() {
        throw new AssertionError("No instances.");
    }

    public static BitSet bitSetFromString(final String s) {
        return BitSet.valueOf(new long[]{Long.parseLong(checkNotEmpty(s), 2)});
    }

    public static BitSet bitSetFromIndexes(final long firstIndex, final long lastIndex) {
        return bitSetFromIndexes((int) firstIndex, (int) lastIndex);
    }

    public static BitSet bitSetFromIndexes(final int firstIndex, final int lastIndex) {
        checkArgument(firstIndex >= 0, "First index can't be negative.");
        checkArgument(lastIndex >= 0, "Last index can't be negative.");

        BitSet bitSet = new BitSet();
        bitSet.set(firstIndex, lastIndex);

        return bitSet;
    }
}
