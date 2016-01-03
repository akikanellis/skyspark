package com.github.dkanellis.skyspark.performance;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.helpers.TextFileToPointRDD;
import com.github.dkanellis.skyspark.performance.parsing.Settings;
import com.github.dkanellis.skyspark.performance.result.PointDataFile;
import com.github.dkanellis.skyspark.performance.result.Result;
import com.github.dkanellis.skyspark.performance.result.ResultWriter;
import com.github.dkanellis.skyspark.performance.result.XmlResultWriter;
import com.google.common.base.Stopwatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.awt.geom.Point2D;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Main {

    private static Stopwatch stopwatch;
    private static Settings settings;
    private static ResultWriter resultWriter;

    public static void main(String[] args) {
        settings = Settings.fromArgs(args);
        if (settings == null) {
            return;
        }

        stopwatch = new Stopwatch();
        resultWriter = new XmlResultWriter(settings.getOutputPath());

        IntStream.rangeClosed(1, settings.getTimes())
                .forEach(t -> settings.getAlgorithms()
                        .forEach(algo -> settings.getPointDataFiles()
                                .forEach(file -> executeWithFile(algo, file))));
    }

    private static void executeWithFile(SkylineAlgorithm skylineAlgorithm, PointDataFile pointDataFile) {
        stopwatch.start();
        SparkConf sparkConf = new SparkConf().setAppName("Performance evaluation for Skyline computing");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        TextFileToPointRDD textFileToPointRDD = new TextFileToPointRDD(sparkContext);
        JavaRDD<Point2D> points = textFileToPointRDD.getPointRddFromTextFile(pointDataFile.getFilePath(), " ");
        JavaRDD<Point2D> skylines = skylineAlgorithm.computeSkylinePoints(points);
        final long numberOfSkylines = skylines.count();
        stopwatch.stop();

        Result result = new Result(skylineAlgorithm.toString(), pointDataFile, stopwatch.elapsed(TimeUnit.MILLISECONDS),
                numberOfSkylines, settings.getNumberOfSlaves(), sparkConf);
        resultWriter.writeResult(result);

        sparkContext.stop();
        stopwatch.reset();
    }
}
