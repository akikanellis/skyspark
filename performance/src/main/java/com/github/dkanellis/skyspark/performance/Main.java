package com.github.dkanellis.skyspark.performance;

import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.TextFileToPointRDD;
import com.github.dkanellis.skyspark.performance.parsing.Settings;
import com.google.common.base.Stopwatch;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;

public class Main {

    private static Stopwatch stopwatch;
    private static TextFileToPointRDD textFileToPointRDD;
    private static Settings settings;
    private static ResultWriter resultWriter;

    public static void main(String[] args) {
        settings = Settings.fromArgs(args);
        if (settings == null) {
            return;
        }

        stopwatch = Stopwatch.createUnstarted();
        textFileToPointRDD = new TextFileToPointRDD(new SparkContextWrapper("perf test", "local[4]"));
        resultWriter = new XmlResultWriter(settings.getOutputPath());

        for (SkylineAlgorithm skylineAlgorithm : settings.getAlgorithms()) {
            executeAlgorithm(skylineAlgorithm);
        }
    }

    private static void executeAlgorithm(SkylineAlgorithm skylineAlgorithm) {
        for (String filePath : settings.getFilepaths()) {
            executeWithFile(skylineAlgorithm, filePath);
        }
    }

    private static void executeWithFile(SkylineAlgorithm skylineAlgorithm, String filePath) {
        for (int timesToRun = 0; timesToRun < settings.getTimes(); ++timesToRun) {
            JavaRDD<Point2D> points = textFileToPointRDD.getPointRDDFromTextFile(filePath, " ");

            stopwatch.start();
            skylineAlgorithm.getSkylinePoints(points);
            stopwatch.stop();

            writeResultToFile(skylineAlgorithm.toString(), stopwatch.elapsedMillis(), filePath);
            stopwatch.reset();
        }
    }

    private static void writeResultToFile(String algorithmName, long elapsedMillis, String filePath) {

    }
}
