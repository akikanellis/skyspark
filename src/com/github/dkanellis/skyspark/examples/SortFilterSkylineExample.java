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

    private final static String DATASETS_SMALL = "data/datasets/small/";
    private final static String DATASETS_MEDIUM = "data/datasets/medium/";
    private final static String DATASETS_BIG = "data/datasets/big/";

    private final static String ANTICORRELATED_SMALL = DATASETS_SMALL + "ANTICOR_2_10000.txt";
    private final static String ANTICORRELATED_MEDIUM = DATASETS_MEDIUM + "ANTICOR_2_100000.txt";
    private final static String ANTICORRELATED_BIG = DATASETS_BIG + "ANTICOR_2_1000000.txt";

    private final static String CORRELATED_SMALL = DATASETS_SMALL + "CORREL_2_10000.txt";
    private final static String CORRELATED_MEDIUM = DATASETS_MEDIUM + "CORREL_2_100000.txt";
    private final static String CORRELATED_BIG = DATASETS_BIG + "CORREL_2_1000000.txt";

    private final static String UNIFORM_SMALL = DATASETS_SMALL + "UNIFORM_2_10000.txt";
    private final static String UNIFORM_MEDIUM = DATASETS_MEDIUM + "UNIFORM_2_100000.txt";
    private final static String UNIFORM_BIG = DATASETS_BIG + "UNIFORM_2_1000000.txt";

    public static void main(String[] args) {
        SparkContextWrapper sparkContext = new SparkContextWrapper("Sort Filter Skyline example", "local[4]");
        SkylineAlgorithmFactory algoritmFactory = new SkylineAlgorithmFactory();
        SkylineAlgorithm sortFilterSkyline = algoritmFactory.getSortFilterSkyline(sparkContext);

        List<Point2D> skylines = sortFilterSkyline.getSkylinePoints(UNIFORM_SMALL);
        print(skylines);
    }

    private static void print(List<Point2D> skylines) {
        int count = 1;
        for (Point2D skyline : skylines) {
            System.out.println(count++ + ": " + skyline);
        }
    }
}
