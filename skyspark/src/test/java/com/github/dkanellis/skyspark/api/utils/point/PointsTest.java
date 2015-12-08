package com.github.dkanellis.skyspark.api.utils.point;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.github.dkanellis.skyspark.api.testUtils.categories.types.UnitTests;

import java.awt.geom.Point2D;

import static org.junit.Assert.*;

@Category(UnitTests.class)
public class PointsTest {

    @Test
    public void firstDominatesSecond() {
        Point2D first = new Point2D.Double(2080.877494624074, 2302.0770958188664);
        Point2D second = new Point2D.Double(5756.202069941658, 4418.941667115589);

        assertTrue(Points.dominates(first, second));
    }

    @Test
    public void firstDoesNotDominateSecond() {
        Point2D first = new Point2D.Double(6803.314583926934, 2266.355737840431);
        Point2D second = new Point2D.Double(5756.202069941658, 4418.941667115589);

        assertFalse(Points.dominates(first, second));
    }

    @Test
    public void returnPointWithBiggestX() {
        Point2D first = new Point2D.Double(6803.314583926934, 2266.355737840431);
        Point2D second = new Point2D.Double(5756.202069941658, 4418.941667115589);

        Point2D expectedPoint = Points.getBiggestPointByXDimension(first, second);

        assertEquals(expectedPoint, first);
    }

    @Test
    public void returnPointWithBiggestY() {
        Point2D first = new Point2D.Double(6803.314583926934, 2266.355737840431);
        Point2D second = new Point2D.Double(5756.202069941658, 4418.941667115589);

        Point2D expectedPoint = Points.getBiggestPointByYDimension(first, second);

        assertEquals(expectedPoint, second);
    }

    @Test
    public void shouldReturnSamePoint2D() {
        String textLine = " 6803.314583926934 2266.355737840431";
        String delimiter = " ";
        Point2D expectedResult = new Point2D.Double(6803.314583926934, 2266.355737840431);

        Point2D result = Points.pointFromTextLine(textLine, delimiter);

        assertEquals(expectedResult, result);
    }
}
