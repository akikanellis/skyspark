package com.akikanellis.skyspark.api.test_utils.data_mocks;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

public class PointsMock {

    private static final List<Point2D> UNIFORM_2_10 = new ArrayList<Point2D>() {{
        add(new Point2D.Double(7.4, 6.4));
        add(new Point2D.Double(5.2, 1.0));
        add(new Point2D.Double(4.3, 8.9));
        add(new Point2D.Double(6.7, 3.8));
        add(new Point2D.Double(7.3, 2.5));
        add(new Point2D.Double(7.6, 9.3));
        add(new Point2D.Double(4.0, 1.5));
        add(new Point2D.Double(1.7, 5.6));
        add(new Point2D.Double(6.4, 3.7));
        add(new Point2D.Double(2.9, 8.4));
    }};

    public static List<Point2D> getUniform210() {
        return new ArrayList<>(UNIFORM_2_10);
    }
}
