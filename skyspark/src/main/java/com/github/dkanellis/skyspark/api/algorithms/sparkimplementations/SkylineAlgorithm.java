package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public interface SkylineAlgorithm {

    public List<Point2D> getSkylinePoints(JavaRDD<Point2D> points);
}
