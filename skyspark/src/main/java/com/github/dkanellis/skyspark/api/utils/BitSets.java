package com.github.dkanellis.skyspark.api.utils;

import java.util.BitSet;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;

public class BitSets {

    public static BitSet bitSetfromString(final String s) {
        return BitSet.valueOf(new long[]{Long.parseLong(checkNotEmpty(s), 2)});
    }
}
