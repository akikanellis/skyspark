package com.github.dkanellis.skyspark.main;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithmFactory;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.Point;
import java.util.List;

/**
 *
 * @author Dimitris Kanellis
 */
public class Main {

    private final static String ANTICORRELATED_SMALL = "data/ANTICOR_2_10000.txt";
    private final static String ANTICORRELATED_MEDIUM = "data/ANTICOR_2_100000.txt";
    private final static String ANTICORRELATED_BIG = "data/ANTICOR_2_1000000.txt";

    private final static String CORRELATED_SMALL = "data/CORREL_2_10000.txt";
    private final static String CORRELATED_MEDIUM = "data/CORREL_2_100000.txt";
    private final static String CORRELATED_BIG = "data/CORREL_2_1000000.txt";

    private final static String UNIFORM_EXTRA_SMALL = "data/UNIFORM_2_100.txt";
    private final static String UNIFORM_SMALL = "data/UNIFORM_2_10000.txt";
    private final static String UNIFORM_MEDIUM = "data/UNIFORM_2_100000.txt";
    private final static String UNIFORM_BIG = "data/UNIFORM_2_1000000.txt";

    public static void main(String[] args) {
        SparkContextWrapper sparkContext = new SparkContextWrapper("Skyline solver", "local");
        SkylineAlgorithmFactory algorithmFactory = new SkylineAlgorithmFactory();

        SkylineAlgorithm bnl = algorithmFactory.getBlockNestedLoop(sparkContext);
        List<Point> skylines = bnl.getSkylinePoints(UNIFORM_EXTRA_SMALL);

        for (Point skyline : skylines) {
            System.out.println(skyline);
        }
    }
}
