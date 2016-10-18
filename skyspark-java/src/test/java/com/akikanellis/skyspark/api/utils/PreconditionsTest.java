package com.akikanellis.skyspark.api.utils;

import org.junit.Test;

import static com.akikanellis.skyspark.api.utils.Preconditions.checkNotEmpty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class PreconditionsTest {

    @Test
    public void checkNotEmpty_whenNull_throwException() {
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> checkNotEmpty(null));
    }

    @Test
    public void checkNotEmpty_whenEmptyString_throwException() {
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> checkNotEmpty(""));
    }

    @Test
    public void checkNotEmpty_whenNormalString_returnString() {
        String expectedString = "A string";
        String actualString = checkNotEmpty(expectedString);

        assertThat(actualString).isEqualTo(expectedString);
    }
}