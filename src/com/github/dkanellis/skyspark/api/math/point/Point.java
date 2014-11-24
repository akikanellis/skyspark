package com.github.dkanellis.skyspark.api.math.point;

import scala.Serializable;

/**
 *
 * @author Dimitris Kanellis
 */
public class Point implements Serializable {

    private final double x;
    private final double y;

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point(String line, String delimiter) {
        line = line.trim();
        String[] lineArray = line.split(delimiter);

        this.x = Double.parseDouble(lineArray[0]);
        this.y = Double.parseDouble(lineArray[1]);
    }

    public boolean dominates(Point point) {
        return (x <= point.getX() && y < point.getY()) || (y <= point.getY() && x < point.getX());
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        Point other = (Point) obj;
        return x == other.getX() && y == other.getY();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 43 * hash + (int) (Double.doubleToLongBits(this.x) ^ (Double.doubleToLongBits(this.x) >>> 32));
        hash = 43 * hash + (int) (Double.doubleToLongBits(this.y) ^ (Double.doubleToLongBits(this.y) >>> 32));
        return hash;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append('(').append(x).append(", ").append(y).append(')');
        return buffer.toString();
    }

}
