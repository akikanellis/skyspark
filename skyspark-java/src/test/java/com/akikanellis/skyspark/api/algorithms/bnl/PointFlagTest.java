package com.akikanellis.skyspark.api.algorithms.bnl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PointFlagTest {

    @Test
    public void equalsIsSymmetric() {
        PointFlag first = new PointFlag(1, 1);
        PointFlag second = new PointFlag(1, 1);

        assertTrue(first.equals(second) && second.equals(first));
        assertTrue(first.hashCode() == second.hashCode());
    }

    @Test
    public void toString_returnData() {
        String expectedName = "[10]";

        String actualName = new PointFlag(1, 0).toString();

        assertEquals(expectedName, actualName);
    }
}
