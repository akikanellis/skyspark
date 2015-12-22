package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.List;

public final class BitmapPointsMock {

    private BitmapPointsMock() {
        throw new AssertionError("No instances.");
    }

    public static JavaRDD<Point2D> get10Points(JavaSparkContext sparkContext) {
        return sparkContext.parallelize(Arrays.asList(
                new Point2D.Double(5.4, 4.4),
                new Point2D.Double(5.0, 4.1),
                new Point2D.Double(3.6, 9.0),
                new Point2D.Double(5.9, 4.0),
                new Point2D.Double(5.9, 4.6),
                new Point2D.Double(2.5, 7.3),
                new Point2D.Double(6.3, 3.5),
                new Point2D.Double(9.9, 4.1),
                new Point2D.Double(6.7, 3.3),
                new Point2D.Double(6.1, 3.4)));
    }

    public static List<Point2D> get10PointsSkylines() {
        return Arrays.asList(
                new Point2D.Double(5.0, 4.1),
                new Point2D.Double(5.9, 4.0),
                new Point2D.Double(6.7, 3.3),
                new Point2D.Double(6.1, 3.4),
                new Point2D.Double(2.5, 7.3));
    }
}
