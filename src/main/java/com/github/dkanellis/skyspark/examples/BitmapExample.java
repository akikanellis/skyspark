package com.github.dkanellis.skyspark.examples;

import com.github.dkanellis.skyspark.api.algorithms.factories.SkylineAlgorithmFactory;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class BitmapExample {

    public static void main(String[] args) {
        SparkContextWrapper sparkContext = new SparkContextWrapper("Bitmap example", "local[4]");
        SkylineAlgorithmFactory algorithmFactory = new SkylineAlgorithmFactory();
        SkylineAlgorithm bitmap = algorithmFactory.getBitmap(sparkContext);

        List<Point2D> skylines = bitmap.getSkylinePoints(Datasets.UNIFORM_SMALL);
        Datasets.print(skylines);
    }
}
