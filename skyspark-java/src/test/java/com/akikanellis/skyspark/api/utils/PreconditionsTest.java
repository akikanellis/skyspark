package com.akikanellis.skyspark.api.utils;

import org.junit.Test;

import static com.akikanellis.skyspark.api.utils.Preconditions.checkNotEmpty;
import static org.junit.Assert.assertEquals;

public class PreconditionsTest {

    @Test(expected = IllegalArgumentException.class)
    public void checkNotEmpty_whenNull_throwException() {
        checkNotEmpty(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkNotEmpty_whenEmptyString_throwException() {
        checkNotEmpty("");
    }

    @Test
    public void checkNotEmpty_whenNormalString_returnString() {
        String expectedString = "A string";
        String actualString = checkNotEmpty(expectedString);

        assertEquals(expectedString, actualString);
    }
}