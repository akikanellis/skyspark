package com.akikanellis.skyspark.api.test_utils;

import com.akikanellis.skyspark.api.helpers.TextFileToPointRDD;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;

public enum DatasetFiles {
    ANTICOR_2_10000,
    ANTICOR_2_100000,
    ANTICOR_2_1000000,
    CORREL_2_10000,
    CORREL_2_100000,
    CORREL_2_1000000,
    UNIFORM_2_10,
    UNIFORM_2_10000,
    UNIFORM_2_100000,
    UNIFORM_2_1000000;

    private final String pointsResourcesPath;
    private final String skylinesResourcesPath;

    DatasetFiles() {
        String pointsRelativePath = String.format("/%s.txt", name());
        String skylinesRelativePath = String.format("/%s_SKYLINES.txt", name());
        this.pointsResourcesPath = getFullPathOfResource(pointsRelativePath);
        this.skylinesResourcesPath = getFullPathOfResource(skylinesRelativePath);
    }

    private String getFullPathOfResource(String name) { return getClass().getResource(name).getFile(); }

    public String pointsPath() { return pointsResourcesPath; }

    public String skylinesPath() { return skylinesResourcesPath; }

    public JavaRDD<Point2D> pointsRdd(TextFileToPointRDD textFileToPointRDD) {
        return textFileToPointRDD.getPointRddFromTextFile(pointsResourcesPath, " ");
    }

    public JavaRDD<Point2D> skylinesRdd(TextFileToPointRDD textFileToPointRDD) {
        return textFileToPointRDD.getPointRddFromTextFile(skylinesResourcesPath, " ");
    }
}
