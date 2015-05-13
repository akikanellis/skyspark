package com.github.dkanellis.skyspark.examples;

import com.github.dkanellis.skyspark.api.algorithms.factories.SkylineAlgorithmFactory;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class SortFilterSkylineExample {

    public static void main(String[] args) {
        SparkContextWrapper sparkContext = new SparkContextWrapper("Sort Filter Skyline example", "local[4]");
        SkylineAlgorithmFactory algoritmFactory = new SkylineAlgorithmFactory();
        SkylineAlgorithm sortFilterSkyline = algoritmFactory.getSortFilterSkyline(sparkContext);

        List<Point2D> skylines = sortFilterSkyline.getSkylinePoints(Datasets.UNIFORM_SMALL);
        Datasets.print(skylines);
    }
}
