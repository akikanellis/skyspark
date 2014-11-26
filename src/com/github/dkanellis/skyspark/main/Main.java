package com.github.dkanellis.skyspark.main;

import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.factories.SkylineAlgorithmFactory;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.Point2DAdvanced;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Dimitris Kanellis
 */
public class Main {

    private final static String DATASETS_FOLDER = "data/datasets/";

    private final static String ANTICORRELATED_SMALL = DATASETS_FOLDER + "ANTICOR_2_10000.txt";
    private final static String ANTICORRELATED_MEDIUM = DATASETS_FOLDER + "ANTICOR_2_100000.txt";
    private final static String ANTICORRELATED_BIG = DATASETS_FOLDER + "ANTICOR_2_1000000.txt";

    private final static String CORRELATED_SMALL = DATASETS_FOLDER + "CORREL_2_10000.txt";
    private final static String CORRELATED_MEDIUM = DATASETS_FOLDER + "CORREL_2_100000.txt";
    private final static String CORRELATED_BIG = DATASETS_FOLDER + "CORREL_2_1000000.txt";

    private final static String UNIFORM_EXTRA_SMALL = DATASETS_FOLDER + "UNIFORM_2_100.txt";
    private final static String UNIFORM_SMALL = DATASETS_FOLDER + "UNIFORM_2_10000.txt";
    private final static String UNIFORM_MEDIUM = DATASETS_FOLDER + "UNIFORM_2_100000.txt";
    private final static String UNIFORM_BIG = DATASETS_FOLDER + "UNIFORM_2_1000000.txt";

    private static void print(List<Point2DAdvanced> points) {
        int count = 1;
        for (Point2DAdvanced point : points) {
            System.out.println(count++ + ": " + point);
        }
    }

    private static void print(List<Point2DAdvanced> first, List<Point2DAdvanced> second) {
        for (int i = 0; i < first.size(); i++) {
            System.out.println(i + ": " + first.get(i) + " - " + second.get(i));
        }
    }

    public static boolean checkCorrectness(List<Point2DAdvanced> points) {
        for (int i = 0; i < points.size(); i++) {
            Point2DAdvanced p1 = points.get(i);
            for (int j = 0; j < points.size(); j++) {
                Point2DAdvanced p2 = points.get(j);
                if (p1.dominates(p2) || p2.dominates(p1)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean checkCorrectness(Iterable<Point2DAdvanced> points) {
        for (Point2DAdvanced point : points) {
            for (Point2DAdvanced other : points) {
                if (point.dominates(other) || other.dominates(point)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean areSame(List<Point2DAdvanced> first, List<Point2DAdvanced> second) {
        if (first.size() != second.size()) {
            return false;
        }

        for (Point2DAdvanced point : first) {
            if (!second.contains(point)) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        SparkContextWrapper sparkContext = new SparkContextWrapper("Skyline solver", "local[4]");
        SkylineAlgorithmFactory algorithmFactory = new SkylineAlgorithmFactory();

        SkylineAlgorithm bnl = algorithmFactory.getBlockNestedLoop(sparkContext);
        SkylineAlgorithm sfs = algorithmFactory.getSortFilterSkyline(sparkContext);
        List<Point2DAdvanced> skylinesBnl = bnl.getSkylinePoints(ANTICORRELATED_SMALL);
        List<Point2DAdvanced> skylinesSfs = sfs.getSkylinePoints(ANTICORRELATED_SMALL);

//        print(skylinesSfs);
//        if (!checkCorrectness(skylinesSfs)) {
//            System.out.println("Not skylines");
//        }
        print(skylinesBnl, skylinesSfs);
        if (!areSame(skylinesBnl, skylinesSfs)) {
            System.out.println("Not same error");
        }
    }
}
