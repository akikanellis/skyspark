package com.github.dkanellis.skyspark.examples;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class Datasets {
    private final static String DATASETS_FOLDER_SMALL = "data/datasets/small/";
    public final static String UNIFORM_SMALL = DATASETS_FOLDER_SMALL + "UNIFORM_2_10000.txt";
    private final static String DATASETS_FOLDER_MEDIUM = "data/datasets/medium/";
    public final static String UNIFORM_MEDIUM = DATASETS_FOLDER_MEDIUM + "UNIFORM_2_100000.txt";
    private final static String DATASETS_FOLDER_BIG = "data/datasets/big/";
    public final static String UNIFORM_BIG = DATASETS_FOLDER_BIG + "UNIFORM_2_1000000.txt";
    private final static String ANTICORRELATED_SMALL = DATASETS_FOLDER_SMALL + "ANTICOR_2_10000.txt";
    private final static String ANTICORRELATED_MEDIUM = DATASETS_FOLDER_MEDIUM + "ANTICOR_2_100000.txt";
    private final static String ANTICORRELATED_BIG = DATASETS_FOLDER_BIG + "ANTICOR_2_1000000.txt";
    private final static String CORRELATED_SMALL = DATASETS_FOLDER_SMALL + "CORREL_2_10000.txt";
    private final static String CORRELATED_MEDIUM = DATASETS_FOLDER_MEDIUM + "CORREL_2_100000.txt";
    private final static String CORRELATED_BIG = DATASETS_FOLDER_BIG + "CORREL_2_1000000.txt";

    public static void print(List<Point2D> points) {
        int count = 1;
        for (Point2D point : points) {
            System.out.println(count++ + ": " + point);
        }
    }
}
