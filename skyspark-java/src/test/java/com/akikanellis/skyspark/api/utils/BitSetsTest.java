package com.akikanellis.skyspark.api.utils;

import org.junit.Test;

import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;

public class BitSetsTest {

    @Test
    public void returnCorrectBitSetBasedOnIndexes() {
        BitSet expectedBitSet = new BitSet(9);
        expectedBitSet.set(2, 8);

        BitSet actualBitSet = BitSets.bitSetFromIndexes(2, 8);

        assertThat(actualBitSet).isEqualTo(expectedBitSet);
    }
}