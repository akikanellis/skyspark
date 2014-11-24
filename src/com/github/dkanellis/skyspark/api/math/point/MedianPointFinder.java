package com.github.dkanellis.skyspark.api.math.point;

import org.apache.spark.api.java.JavaRDD;

/**
 *
 * @author Dimitris Kanellis
 */
public class MedianPointFinder {

    public static Point findMedianPoint(JavaRDD<Point> points) {
        Point biggestPointByXDimension = points.reduce((a, b) -> getBiggestPointByXDimension(a, b));
        Point biggestPointByYDimension = points.reduce((a, b) -> getBiggestPointByYDimension(a, b));

        double xDimensionMedian = biggestPointByXDimension.getX() / 2.0;
        double yDimensionMedian = biggestPointByYDimension.getY() / 2.0;

        return new Point(xDimensionMedian, yDimensionMedian);
    }

    private static Point getBiggestPointByXDimension(Point first, Point second) {
        return first.getX() > second.getX() ? first : second;
    }

    private static Point getBiggestPointByYDimension(Point first, Point second) {
        return first.getY() > second.getY() ? first : second;
    }
}
