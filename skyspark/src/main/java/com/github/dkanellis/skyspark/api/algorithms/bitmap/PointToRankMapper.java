package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.List;

public class PointToRankMapper implements Serializable {

    public JavaPairRDD<Point2D, List<Tuple2<Integer, Integer>>> mapPointsToTheirRankings(
            JavaPairRDD<Tuple2<Integer, Double>, Point2D> dimensionValuePoint,
            JavaPairRDD<Tuple2<Integer, Double>, Integer> dimensionWithValueRanking) {

        JavaPairRDD<Point2D, List<Tuple2<Integer, Integer>>> flattenedWithRankings
                = getPointsWithDimensionsRankings(dimensionValuePoint, dimensionWithValueRanking);

        return flattenedWithRankings;
    }

    private JavaPairRDD<Point2D, List<Tuple2<Integer, Integer>>> getPointsWithDimensionsRankings(
            JavaPairRDD<Tuple2<Integer, Double>, Point2D> dimensionValuePoint,
            JavaPairRDD<Tuple2<Integer, Double>, Integer> dimensionValueRanking) {

        return dimensionValuePoint.join(dimensionValueRanking)
                .mapToPair(p -> new Tuple2<>(p._2()._1(), new Tuple2<>(p._1()._1(), p._2()._2())))
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1(), Lists.newArrayList(p._2())));
    }
}
