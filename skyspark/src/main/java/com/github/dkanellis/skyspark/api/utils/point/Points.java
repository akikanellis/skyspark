package com.github.dkanellis.skyspark.api.utils.point;

import java.awt.geom.Point2D;

/**
 * Utility class for {@link Point2D}
 */
public final class Points {

    private Points() {
        throw new AssertionError("No instances.");
    }

    public static boolean dominates(Point2D p, Point2D q) {
        Double x1 = p.getX();
        Double y1 = p.getY();
        Double x2 = q.getX();
        Double y2 = q.getY();

        return (x1 <= x2 && y1 < y2) || (y1 <= y2 && x1 < x2);
    }

    public static Point2D pointFromTextLine(String textLine, String delimiter) {
        textLine = textLine.trim();
        String[] lineArray = textLine.split(delimiter);
        double x = java.lang.Double.parseDouble(lineArray[0]);
        double y = java.lang.Double.parseDouble(lineArray[1]);
        return new Point2D.Double(x, y);
    }

    public static Point2D getBiggestPointByXDimension(Point2D first, Point2D second) {
        return first.getX() > second.getX() ? first : second;
    }

    public static Point2D getBiggestPointByYDimension(Point2D first, Point2D second) {
        return first.getY() > second.getY() ? first : second;
    }
}
