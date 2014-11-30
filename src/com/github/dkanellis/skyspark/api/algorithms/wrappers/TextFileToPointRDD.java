package com.github.dkanellis.skyspark.api.algorithms.wrappers;

import com.github.dkanellis.skyspark.api.math.point.PointUtils;
import java.awt.geom.Point2D;
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

    public JavaRDD<Point2D> getPointRDDFromTextFile(String filePath, String delimiter) {
        JavaRDD<String> lines = sparkContext.textFile(filePath);
        JavaRDD<Point2D> points = convertToPoints(lines, delimiter);
        return points;
    }

    private JavaRDD<Point2D> convertToPoints(JavaRDD<String> lines, String delimiter) {
        return lines.map(line -> PointUtils.pointFromTextLine(line, delimiter));
    }
}
