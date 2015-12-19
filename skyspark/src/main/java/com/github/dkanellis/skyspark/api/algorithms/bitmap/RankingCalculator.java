package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;

interface RankingCalculator {

    JavaPairRDD<Double, Long> computeDistinctRankings(JavaRDD<Double> dimensionValues);

    JavaPairRDD<Rankings, Point2D> applyRankingsToAllPoints(JavaRDD<Point2D> points,
                                                            JavaPairRDD<Double, Long> firstDimensionRanking,
                                                            JavaPairRDD<Double, Long> secondDimensionRanking);

    JavaPairRDD<Rankings, Point2D> getWithPreviousRankingsPoints(JavaPairRDD<Rankings, Point2D> pointsWithRankings);
}
