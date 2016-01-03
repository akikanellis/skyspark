package com.github.dkanellis.skyspark.examples;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.bnl.BlockNestedLoop;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SkySpark examples").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Point2D> pointsList = Arrays.asList(
                new Point2D.Double(7384.475430902016, 6753.440980966086),
                new Point2D.Double(5132.274536706283, 176.07686372999513),
                new Point2D.Double(4691.353681108572, 8232.92056883113),
                new Point2D.Double(6292.759807632455, 3790.860474769524),
                new Point2D.Double(7250.357537607312, 2525.5019336970545),
                new Point2D.Double(755.6005425518632, 9555.368016130034),
                new Point2D.Double(4901.088081890441, 1031.5393572675114),
                new Point2D.Double(1963.7806084781564, 5979.674156591443),
                new Point2D.Double(6676.451493576449, 3373.7938261245263),
                new Point2D.Double(2604.9875068505676, 8312.417734885097)
        );

        JavaRDD<Point2D> pointRdd = sc.parallelize(pointsList);

        SkylineAlgorithm skylineAlgorithm;

        skylineAlgorithm = new BlockNestedLoop();
        //skylineAlgorithm = new SortFilterSkyline();
        //skylineAlgorithm = new Bitmap();

        JavaRDD<Point2D> skylines = skylineAlgorithm.computeSkylinePoints(pointRdd);
        List<Point2D> skylineList = skylines.collect();

        System.out.println("Input set is:");
        pointsList.forEach(System.out::println);

        System.out.println("Skyline set is:");
        skylineList.forEach(System.out::println);
    }
}
