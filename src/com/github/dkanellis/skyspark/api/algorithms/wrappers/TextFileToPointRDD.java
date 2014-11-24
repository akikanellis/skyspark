package com.github.dkanellis.skyspark.api.algorithms.wrappers;

import com.github.dkanellis.skyspark.api.math.point.Point;
import org.apache.spark.api.java.JavaRDD;

/**
 *
 * @author Dimitris Kanellis
 */
public class TextFileToPointRDD {

    private final SparkContextWrapper sparkContext;

    public TextFileToPointRDD(SparkContextWrapper sparkContext) {
        this.sparkContext = sparkContext;
    }

    public JavaRDD<Point> getPointRDDFromTextFile(String filePath, String delimiter) {
        JavaRDD<String> lines = sparkContext.textFile(filePath);
        JavaRDD<Point> points = convertToPoints(lines, delimiter);
        return points;
    }

    private JavaRDD<Point> convertToPoints(JavaRDD<String> lines, String delimiter) {
        return lines.map(line -> new Point(line, delimiter));
    }
}
