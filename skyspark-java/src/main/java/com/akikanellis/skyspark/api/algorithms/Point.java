package com.akikanellis.skyspark.api.algorithms;

import java.io.Serializable;
import java.util.Arrays;

public final class Point implements Serializable {
    private final double[] dimensions;

    public Point(double... dimensions) { this.dimensions = dimensions; }

    /**
     * The number of dimensions in this point.
     * <p>
     * For example the 2-dimensional point <i>Point(2.5, 5.3)</i> will return 2.
     * <p>
     * The 3-dimensional point <i>Point(6.7, 2.5, 5.3)</i> will return 3.
     *
     * @return The number of dimensions in this point
     */
    public int size() { return dimensions.length; }

    /**
     * The value of a point's dimension.
     * <p>
     * For example:
     * {{{
     * Point point = new Point(2.5, 5.3)
     * System.out.print(point.dimension(0)) // Prints 2.5
     * }}}
     *
     * @param i The index of the dimension
     * @return The value of the dimension
     */
    public double dimension(int i) { return dimensions[i]; }

    /**
     * Checks if this point dominates another one by using the specified dominating algorithm.
     *
     * @param other The point to check against
     * @return True if this point dominates the other, false if not
     */
    public boolean dominates(Point other) { throw new UnsupportedOperationException("To be implemented."); }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Point other = (Point) o;
        return Arrays.equals(dimensions, other.dimensions);
    }

    @Override public int hashCode() { return Arrays.hashCode(dimensions); }
}
