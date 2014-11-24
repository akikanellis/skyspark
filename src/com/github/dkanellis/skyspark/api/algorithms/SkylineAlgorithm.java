package com.github.dkanellis.skyspark.api.algorithms;

import com.github.dkanellis.skyspark.api.math.point.Point;
import java.util.List;

/**
 *
 * @author Dimitris Kanellis
 */
public interface SkylineAlgorithm {

    public List<Point> getSkylinePoints(String filepath);
}
