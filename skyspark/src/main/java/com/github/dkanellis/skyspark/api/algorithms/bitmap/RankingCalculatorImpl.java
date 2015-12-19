package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;

public class RankingCalculatorImpl implements RankingCalculator {

    private final int numberOfPartitions;

    public RankingCalculatorImpl(final int numberOfPartitions) {
        this.numberOfPartitions = numberOfPartitions;
    }

    @Override
    public JavaPairRDD<Double, Long> computeDistinctRankings(JavaRDD<Double> dimensionValues) {
        return dimensionValues
                .distinct()
                .sortBy(Double::doubleValue, true, numberOfPartitions)
                .zipWithIndex();
    }

    @Override
    public JavaPairRDD<Rankings, Point2D> applyRankingsToAllPoints(JavaRDD<Point2D> points,
                                                                   JavaPairRDD<Double, Long> firstDimensionRanking,
                                                                   JavaPairRDD<Double, Long> secondDimensionRanking) {
        JavaPairRDD<Point2D, Long> pointsWithRankingsOfFirstDimension
                = points.keyBy(Point2D::getX)
                .join(firstDimensionRanking)
                .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2()));

        JavaPairRDD<Point2D, Long> pointsWithRankingsOfSecondDimension
                = points.keyBy(Point2D::getY)
                .join(secondDimensionRanking)
                .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2()));

        return pointsWithRankingsOfFirstDimension
                .join(pointsWithRankingsOfSecondDimension)
                .mapToPair(p -> new Tuple2<>(Rankings.fromTuple(p._2()), p._1()));
    }

    @Override
    public JavaPairRDD<Rankings, Point2D> getWithPreviousRankingsPoints(JavaPairRDD<Rankings, Point2D> pointsWithRankings) {
        return pointsWithRankings.mapToPair(p -> new Tuple2<>(Rankings.reduceByOne(p._1()), p._2()));
    }
}
