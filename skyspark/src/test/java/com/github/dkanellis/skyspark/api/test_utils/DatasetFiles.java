package com.github.dkanellis.skyspark.api.test_utils;

import com.github.dkanellis.skyspark.api.helpers.TextFileToPointRDD;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;

public class DatasetFiles {

    private static String DELIMITER = " ";

    public static JavaRDD<Point2D> getRddFromFile(TextFileToPointRDD textFileToPointRDD, String filePath) {
        String fullFilePath = DatasetFiles.class.getResource(filePath).getPath();
        return textFileToPointRDD.getPointRDDFromTextFile(fullFilePath, DELIMITER);
    }
}
