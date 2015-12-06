package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.math.point.FlagPointPairProducer;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class Bitmap implements SkylineAlgorithm {

    private FlagPointPairProducer flagPointPairProducer;

    @Override
    public List<Point2D> getSkylinePoints(JavaRDD<Point2D> points) {
        throw new UnsupportedOperationException("Bitmap is not supported yet.");
    }
}
