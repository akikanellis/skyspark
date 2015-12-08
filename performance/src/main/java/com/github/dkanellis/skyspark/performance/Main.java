package com.github.dkanellis.skyspark.performance;

import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.TextFileToPointRDD;
import com.github.dkanellis.skyspark.performance.parsing.Settings;
import com.github.dkanellis.skyspark.performance.result.PointDataFile;
import com.github.dkanellis.skyspark.performance.result.Result;
import com.github.dkanellis.skyspark.performance.result.ResultWriter;
import com.github.dkanellis.skyspark.performance.result.XmlResultWriter;
import com.google.common.base.Stopwatch;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {

    private static Stopwatch stopwatch;
    private static TextFileToPointRDD textFileToPointRDD;
    private static Settings settings;
    private static ResultWriter resultWriter;
    private static List<Point2D> skylines;

    public static void main(String[] args) {
        settings = Settings.fromArgs(args);
        if (settings == null) {
            return;
        }

        stopwatch = new Stopwatch();
        textFileToPointRDD = new TextFileToPointRDD(new SparkContextWrapper("perf test", "local[4]"));
        resultWriter = new XmlResultWriter(settings.getOutputPath());

        for (SkylineAlgorithm skylineAlgorithm : settings.getAlgorithms()) {
            executeAlgorithm(skylineAlgorithm);
        }
    }

    private static void executeAlgorithm(SkylineAlgorithm skylineAlgorithm) {
        for (PointDataFile pointDataFile : settings.getPointDataFiles()) {
            executeWithFile(skylineAlgorithm, pointDataFile);
        }
    }

    private static void executeWithFile(SkylineAlgorithm skylineAlgorithm, PointDataFile pointDataFile) {
        for (int timesToRun = 0; timesToRun < settings.getTimes(); ++timesToRun) {
            JavaRDD<Point2D> points = textFileToPointRDD.getPointRDDFromTextFile(pointDataFile.getFilePath(), " ");

            stopwatch.start();
            skylines = skylineAlgorithm.getSkylinePoints(points);
            stopwatch.stop();

            Result result = new Result(skylineAlgorithm.toString(), pointDataFile,
                    stopwatch.elapsed(TimeUnit.NANOSECONDS), skylines.size());
            resultWriter.writeResult(result);

            stopwatch.reset();
        }
    }
}
