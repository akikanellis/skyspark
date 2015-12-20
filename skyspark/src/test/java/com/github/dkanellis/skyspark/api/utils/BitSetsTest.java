package com.github.dkanellis.skyspark.api.utils;

import org.junit.Test;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;

public class BitSetsTest {

    @Test
    public void returnCorrectBitSetBasedOnIndexes() {
        BitSet expectedBitSet = new BitSet(9);
        expectedBitSet.set(2, 8);

        BitSet actualBitSet = BitSets.bitSetFromIndexes(2, 8);

        assertEquals(expectedBitSet, actualBitSet);
    }
}