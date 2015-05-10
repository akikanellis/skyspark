package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public interface SkylineAlgorithm {

    List<Point2D> getSkylinePoints(String filepath);
}
