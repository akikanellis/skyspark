package com.akikanellis.skyspark.api.utils.point;

import com.akikanellis.skyspark.api.test_utils.categories.types.UnitTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.awt.geom.Point2D;

import static org.junit.Assert.assertTrue;

@Category(UnitTests.class)
public class PointDominationComparatorMinAnnotationTest {

    private PointDominationComparatorMinAnnotation comparator;

    @Before
    public void setUp() {
        comparator = new PointDominationComparatorMinAnnotation();
    }

    @Test
    public void equals() {
        Point2D first = new Point2D.Double(634.423, 542.11);
        Point2D second = new Point2D.Double(634.423, 542.11);

        final int result = comparator.compare(first, second);

        assertTrue(result == 0);
    }

    @Test
    public void lessThan() {
        Point2D first = new Point2D.Double(502.345, 349.12);
        Point2D second = new Point2D.Double(634.423, 542.11);

        final int result = comparator.compare(first, second);

        assertTrue(result <= -1);
    }

    @Test
    public void greaterThan() {
        Point2D first = new Point2D.Double(634.423, 542.11);
        Point2D second = new Point2D.Double(502.345, 349.12);

        final int result = comparator.compare(first, second);

        assertTrue(result >= 1);
    }

    @Test
    public void equalsReversed() {
        Point2D first = new Point2D.Double(634.423, 542.11);
        Point2D second = new Point2D.Double(542.11, 634.423);

        final int result = comparator.compare(first, second);

        assertTrue(result == 0);
    }
}
