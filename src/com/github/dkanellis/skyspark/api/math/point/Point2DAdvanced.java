package com.github.dkanellis.skyspark.api.math.point;

import java.awt.geom.Point2D;
import scala.Serializable;

/**
 *
 * @author Dimitris Kanellis
 */
public class Point2DAdvanced extends Point2D.Double implements Serializable {

    public Point2DAdvanced(double x, double y) {
        super(x, y);
    }

    public boolean dominates(Point2D.Double point) {
        return (x <= point.getX() && y < point.getY())
                || (y <= point.getY() && x < point.getX());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Point2DAdvanced)) {
            return false;
        }
        
        Point2D other = (Point2D) o;
        return (x == other.getX()) && (y == other.getY());
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('(').append(x).append(", ").append(y).append(')');
        return builder.toString();
    }

    public static Point2DAdvanced fromTextLine(String textLine, String delimiter) {
        textLine = textLine.trim();
        String[] lineArray = textLine.split(delimiter);
        double x = java.lang.Double.parseDouble(lineArray[0]);
        double y = java.lang.Double.parseDouble(lineArray[1]);
        return new Point2DAdvanced(x, y);
    }
}
