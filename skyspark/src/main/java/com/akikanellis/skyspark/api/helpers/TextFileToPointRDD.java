package com.akikanellis.skyspark.api.helpers;

import com.akikanellis.skyspark.api.utils.point.Points;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.awt.geom.Point2D;

/**
 * Helper class to convert a text file to a JavaRDD<Point2D>
 */
public class TextFileToPointRDD {

    private final JavaSparkContext sparkContext;

    public TextFileToPointRDD(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public JavaRDD<Point2D> getPointRddFromTextFile(String filePath, String delimiter) {
        JavaRDD<String> lines = sparkContext.textFile(filePath);
        return convertToPoints(lines, delimiter);
    }

    private JavaRDD<Point2D> convertToPoints(JavaRDD<String> lines, String delimiter) {
        return lines.map(line -> Points.pointFromTextLine(line, delimiter));
    }
}
