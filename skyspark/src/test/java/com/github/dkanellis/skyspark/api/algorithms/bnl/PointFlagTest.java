package com.github.dkanellis.skyspark.api.algorithms.bnl;

import com.github.dkanellis.skyspark.api.test_utils.categories.types.UnitTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTests.class)
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
