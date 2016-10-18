package com.akikanellis.skyspark.api.algorithms.bnl;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PointFlagTest {

    @Test
    public void equalsIsSymmetric() {
        PointFlag first = new PointFlag(1, 1);
        PointFlag second = new PointFlag(1, 1);

        assertThat(first).isEqualTo(second);
        assertThat(second).isEqualTo(first);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }

    @Test
    public void toString_returnData() {
        String expectedName = "[10]";

        String actualName = new PointFlag(1, 0).toString();

        assertThat(actualName).isEqualTo(expectedName);
    }
}
