package com.github.dkanellis.skyspark.api.test_utils.mocks;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public final class PointMock {

    private PointMock() {
        throw new AssertionError("No instances.");
    }

    public static List<Point2D> get10Points() {
        List<Point2D> points = new ArrayList<>();

        points.add(new Point2D.Double(5.4, 4.4));
        points.add(new Point2D.Double(5.0, 4.1));
        points.add(new Point2D.Double(3.6, 9.0));
        points.add(new Point2D.Double(5.9, 4.0));
        points.add(new Point2D.Double(5.9, 4.6));
        points.add(new Point2D.Double(2.5, 7.3));
        points.add(new Point2D.Double(6.3, 3.5));
        points.add(new Point2D.Double(9.9, 4.1));
        points.add(new Point2D.Double(6.7, 3.3));
        points.add(new Point2D.Double(6.1, 3.4));

        return points;
    }

    public static List<Point2D> get10PointsSkylines() {
        List<Point2D> points = new ArrayList<>();

        points.add(new Point2D.Double(5.0, 4.1));
        points.add(new Point2D.Double(5.9, 4.0));
        points.add(new Point2D.Double(6.7, 3.3));
        points.add(new Point2D.Double(6.1, 3.4));
        points.add(new Point2D.Double(2.5, 7.3));

        return points;
    }

    public static List<BitSet> get100PointsBitVectors() {
        List<BitSet> bitVectors = new ArrayList<>();

        return null;
    }
}
