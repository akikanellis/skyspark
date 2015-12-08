package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.bnl.FlagPointPairProducer;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class Bitmap implements com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm {

    private FlagPointPairProducer flagPointPairProducer;

    @Override
    public List<Point2D> getSkylinePoints(JavaRDD<Point2D> points) {
        throw new UnsupportedOperationException("Bitmap is not supported yet.");
    }

    @Override
    public String toString() {
        return "Bitmap";
    }
}
