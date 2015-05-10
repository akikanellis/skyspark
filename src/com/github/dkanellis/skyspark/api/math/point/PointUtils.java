package com.github.dkanellis.skyspark.api.math.point;

import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;

/**
 * @author Dimitris Kanellis
 */
public class PointUtils {

    public static boolean dominates(Point2D p, Point2D q) {
        double x1 = p.getX();
        double y1 = p.getY();
        double x2 = q.getX();
        double y2 = q.getY();

        return (x1 <= x2 && y1 < y2) || (y1 <= y2 && x1 < x2);
    }

    public static Point2D pointFromTextLine(String textLine, String delimiter) {
        textLine = textLine.trim();
        String[] lineArray = textLine.split(delimiter);
        double x = java.lang.Double.parseDouble(lineArray[0]);
        double y = java.lang.Double.parseDouble(lineArray[1]);
        return new Point2D.Double(x, y);
    }

    public static Point2D getMedianPointFromRDD(JavaRDD<Point2D> points) {
        Point2D biggestPointByXDimension = points.reduce(PointUtils::getBiggestPointByXDimension);
        Point2D biggestPointByYDimension = points.reduce(PointUtils::getBiggestPointByYDimension);

        double xDimensionMedian = biggestPointByXDimension.getX() / 2.0;
        double yDimensionMedian = biggestPointByYDimension.getY() / 2.0;

        return new Point2D.Double(xDimensionMedian, yDimensionMedian);
    }

    private static Point2D getBiggestPointByXDimension(Point2D first, Point2D second) {
        return first.getX() > second.getX() ? first : second;
    }

    private static Point2D getBiggestPointByYDimension(Point2D first, Point2D second) {
        return first.getY() > second.getY() ? first : second;
    }
}
