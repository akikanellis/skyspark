package com.github.dkanellis.skyspark.api.utils;

import com.github.dkanellis.skyspark.api.test_utils.categories.speeds.FastTests;
import com.github.dkanellis.skyspark.api.test_utils.categories.types.UnitTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;
import static org.junit.Assert.assertEquals;

@Category({UnitTests.class, FastTests.class})
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