package com.github.dkanellis.skyspark.performance;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.helpers.TextFileToPointRDD;
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

    public static void main(String[] args) {
        settings = Settings.fromArgs(args);
        if (settings == null) {
            return;
        }

        stopwatch = new Stopwatch();
        textFileToPointRDD = new TextFileToPointRDD(new SparkContextWrapper());
        resultWriter = new XmlResultWriter(settings.getOutputPath());

        settings.getAlgorithms().forEach(Main::executeAlgorithm);
    }

    private static void executeAlgorithm(SkylineAlgorithm skylineAlgorithm) {
        for (PointDataFile pointDataFile : settings.getPointDataFiles()) {
            executeWithFile(skylineAlgorithm, pointDataFile);
        }
    }

    private static void executeWithFile(SkylineAlgorithm skylineAlgorithm, PointDataFile pointDataFile) {
        for (int timesToRun = 0; timesToRun < settings.getTimes(); ++timesToRun) {
            JavaRDD<Point2D> points = textFileToPointRDD.getPointRddFromTextFile(pointDataFile.getFilePath(), " ");

            stopwatch.start();
            JavaRDD<Point2D> skylines = skylineAlgorithm.computeSkylinePoints(points);
            List<Point2D> skylineList = skylines.collect();
            stopwatch.stop();

            Result result = new Result(skylineAlgorithm.toString(), pointDataFile,
                    stopwatch.elapsed(TimeUnit.NANOSECONDS), skylineList.size());
            resultWriter.writeResult(result);

            stopwatch.reset();
        }
    }
}
