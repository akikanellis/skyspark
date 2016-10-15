package com.akikanellis.skyspark.api.utils;

import com.akikanellis.skyspark.api.test_utils.categories.types.UnitTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;

@Category(UnitTests.class)
public class BitSetsTest {

    @Test
    public void returnCorrectBitSetBasedOnIndexes() {
        BitSet expectedBitSet = new BitSet(9);
        expectedBitSet.set(2, 8);

        BitSet actualBitSet = BitSets.bitSetFromIndexes(2, 8);

        assertEquals(expectedBitSet, actualBitSet);
    }
}