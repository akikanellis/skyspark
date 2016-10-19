package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

class MedianFinder implements Serializable {

    Point getMedian(JavaRDD<Point> points) {
        JavaPairRDD<Integer, Iterable<Double>> dimensionValuesGrouped = points
                .flatMapToPair(this::extractDimensions)
                .groupByKey();

        JavaPairRDD<Integer, Double> maxesOfEachDimension = dimensionValuesGrouped
                .mapValues(dimensions -> findMax(dimensions) / 2);

        return maxesOfEachDimension
                .coalesce(1)
                .glom()
                .map(this::tuplesToPoint)
                .first();
    }

    private Double findMax(Iterable<Double> dimensions) {
        return StreamSupport.stream(dimensions.spliterator(), true)
                .max(Double::compare)
                .orElseThrow(() -> new IllegalArgumentException("The point was empty"));
    }

    private Iterable<Tuple2<Integer, Double>> extractDimensions(Point point) {
        return IntStream.range(0, point.size())
                .mapToObj(i -> new Tuple2<>(i, point.dimension(i)))
                .collect(Collectors.toList());
    }

    private Point tuplesToPoint(Collection<Tuple2<Integer, Double>> tuples){
        double[] dimensions = new double[tuples.size()];
        tuples.forEach(t -> {
            int index = t._1();
            double value = t._2();

            dimensions[index] = value;
        });

        return new Point(dimensions);
    }
}
