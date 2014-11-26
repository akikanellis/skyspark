package com.github.dkanellis.skyspark.api.math.point;

import org.apache.spark.api.java.JavaRDD;

/**
 *
 * @author Dimitris Kanellis
 */
public class MedianPointFinder {

    public static Point2DAdvanced findMedianPoint(JavaRDD<Point2DAdvanced> points) {
        Point2DAdvanced biggestPointByXDimension = points.reduce((a, b) -> getBiggestPointByXDimension(a, b));
        Point2DAdvanced biggestPointByYDimension = points.reduce((a, b) -> getBiggestPointByYDimension(a, b));

        double xDimensionMedian = biggestPointByXDimension.getX() / 2.0;
        double yDimensionMedian = biggestPointByYDimension.getY() / 2.0;

        return new Point2DAdvanced(xDimensionMedian, yDimensionMedian);
    }

    private static Point2DAdvanced getBiggestPointByXDimension(Point2DAdvanced first, Point2DAdvanced second) {
        return first.getX() > second.getX() ? first : second;
    }

    private static Point2DAdvanced getBiggestPointByYDimension(Point2DAdvanced first, Point2DAdvanced second) {
        return first.getY() > second.getY() ? first : second;
    }
}
