package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.TextFileToPointRDD;
import com.github.dkanellis.skyspark.api.math.point.FlagPointPairProducer;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class Bitmap implements SkylineAlgorithm {

    private final transient TextFileToPointRDD txtToPoints;
    private FlagPointPairProducer flagPointPairProducer;

    public Bitmap(SparkContextWrapper sparkContext) {
        this.txtToPoints = new TextFileToPointRDD(sparkContext);
    }

    @Override
    public List<Point2D> getSkylinePoints(String filepath) {
        throw new UnsupportedOperationException("Bitmap is not supported yet.");
    }
}
