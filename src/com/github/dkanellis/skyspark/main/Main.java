package com.github.dkanellis.skyspark.main;

import com.github.dkanellis.skyspark.api.algorithms.factories.SkylineAlgorithmFactory;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.PointUtils;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

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

    public static void main(String[] args) {
        SparkContextWrapper sparkContext = new SparkContextWrapper("Skyline solver", "local[4]");
        SkylineAlgorithmFactory algorithmFactory = new SkylineAlgorithmFactory();
        List<String> files = createFileList();
        String file = ANTICORRELATED_BIG;

        SkylineAlgorithm bnl = algorithmFactory.getBlockNestedLoop(sparkContext);
        SkylineAlgorithm sfs = algorithmFactory.getSortFilterSkyline(sparkContext);
        long startTime = System.nanoTime();
        List<Point2D> skylinesBnl = bnl.getSkylinePoints(file);
        long endTime = System.nanoTime();

        long durationBnl = (endTime - startTime) / 1000000;
        startTime = System.nanoTime();
        List<Point2D> skylinesSfs = sfs.getSkylinePoints(file);
        endTime = System.nanoTime();
        long durationSfs = (endTime - startTime) / 1000000;
        System.out.println("BNL: " + durationBnl + " ms - size: " + skylinesBnl.size());
        System.out.println("SFS: " + durationSfs + " ms - size: " + skylinesSfs.size());
    }

    private static void print(List<Point2D> points) {
        int count = 1;
        for (Point2D point : points) {
            //System.out.println(count++ + ": " + point);
            System.out.println(point);
        }
    }

    private static void print(List<Point2D> first, List<Point2D> second) {
        for (int i = 0; i < first.size(); i++) {
            System.out.println(i + ": " + first.get(i) + " - " + second.get(i));
        }
    }

    public static boolean checkCorrectness(List<Point2D> points) {
        for (int i = 0; i < points.size(); i++) {
            Point2D p1 = points.get(i);
            for (int j = 0; j < points.size(); j++) {
                Point2D p2 = points.get(j);
                if (PointUtils.dominates(p1, p2) || PointUtils.dominates(p2, p1)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean checkCorrectness(Iterable<Point2D> points) {
        for (Point2D point : points) {
            for (Point2D other : points) {
                if (PointUtils.dominates(point, other) || PointUtils.dominates(other, point)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean areSame(List<Point2D> first, List<Point2D> second) {
        if (first.size() != second.size()) {
            return false;
        }

        for (Point2D point : first) {
            if (!second.contains(point)) {
                return false;
            }
        }
        return true;
    }

    private static List<String> createFileList() {
        List<String> files = new ArrayList<>();
        files.add(ANTICORRELATED_SMALL);
        files.add(ANTICORRELATED_MEDIUM);
        files.add(ANTICORRELATED_BIG);
        files.add(CORRELATED_SMALL);
        files.add(CORRELATED_MEDIUM);
        files.add(CORRELATED_BIG);
        files.add(UNIFORM_SMALL);
        files.add(UNIFORM_MEDIUM);
        files.add(UNIFORM_BIG);
        return files;
    }
}
