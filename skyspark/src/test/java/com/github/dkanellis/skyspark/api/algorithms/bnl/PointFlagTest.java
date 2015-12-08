package com.github.dkanellis.skyspark.api.algorithms.bnl;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.github.dkanellis.skyspark.api.testUtils.categories.types.UnitTests;

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
}
