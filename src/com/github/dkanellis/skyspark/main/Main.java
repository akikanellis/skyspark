package com.github.dkanellis.skyspark.main;

import com.github.dkanellis.skyspark.api.algorithms.factories.SkylineAlgorithmFactory;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.PointUtils;
import com.github.dkanellis.skyspark.performance.PerformanceResult;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertTrue;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

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
        // Run one of the test suites
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
