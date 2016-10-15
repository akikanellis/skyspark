package com.akikanellis.skyspark.api.algorithms;

import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.io.Serializable;

/**
 * A skyline algorithm is an algorithm capable of calculating the skyline set of a dataset S. Current implementations
 * include {@link com.akikanellis.skyspark.api.algorithms.bitmap.Bitmap},
 * {@link com.akikanellis.skyspark.api.algorithms.bnl.BlockNestedLoop}
 * and {@link com.akikanellis.skyspark.api.algorithms.sfs.SortFilterSkyline}
 */
public interface SkylineAlgorithm extends Serializable {

    /**
     * Given an RDD of points, return the skyline set of them.
     *
     * @param points the points to give the skyline for.
     * @return the skyline set.
     */
    JavaRDD<Point2D> computeSkylinePoints(JavaRDD<Point2D> points);
}
