package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.math.point.Point2DAdvanced;
import java.util.List;

/**
 *
 * @author Dimitris Kanellis
 */
public interface SkylineAlgorithm {

    public List<Point2DAdvanced> getSkylinePoints(String filepath);
}
