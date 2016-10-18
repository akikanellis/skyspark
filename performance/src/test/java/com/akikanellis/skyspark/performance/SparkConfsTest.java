package com.akikanellis.skyspark.performance;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkConfsTest {

    @Test
    public void usingM_getInGigabyte() {
        String memoryString = "6004m";
        double expectedAmount = 5.86;

        double actualAmount = SparkConfs.memoryStringToGigabytes(memoryString);

        assertThat(actualAmount).isEqualTo(expectedAmount);
    }


    @Test
    public void usingG_getInGigabyte() {
        String memoryString = "6.2g";
        double expectedAmount = 6.2;

        double actualAmount = SparkConfs.memoryStringToGigabytes(memoryString);

        assertThat(actualAmount).isEqualTo(expectedAmount);
    }
}