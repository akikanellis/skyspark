package com.github.dkanellis.skyspark.api.algorithms;

import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.io.Serializable;

public interface SkylineAlgorithm extends Serializable {
    JavaRDD<Point2D> computeSkylinePoints(JavaRDD<Point2D> points);
}
