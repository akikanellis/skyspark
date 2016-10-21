package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;

class FlagAdder implements Serializable {
    private final MedianFinder medianFinder;

    FlagAdder(MedianFinder medianFinder) { this.medianFinder = medianFinder; }

    JavaPairRDD<Flag, Point> addFlags(JavaRDD<Point> points) {
        Point median = medianFinder.getMedian(points);
        FlagProducer flagProducer = createFlagProducer(median);

        return points.mapToPair(p -> new Tuple2<>(flagProducer.calculateFlag(p), p));
    }

    protected FlagProducer createFlagProducer(Point median) { return new FlagProducer(median); }
}
