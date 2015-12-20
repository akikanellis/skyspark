package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.awt.geom.Point2D;
import java.util.BitSet;

public class PointsWithBitmapMergerImpl implements PointsWithBitmapMerger {
    @Override
    public JavaRDD<PointsWithRequiredBitSlices> mergePointsWithBitmap(@NotNull JavaPairRDD<Rankings, Point2D> pointsWithRankings,
                                                                      @NotNull JavaPairRDD<Rankings, Point2D> pointsWithPreviousRankings,
                                                                      @NotNull JavaPairRDD<Long, BitSet> bitmapOfFirstDimension,
                                                                      @NotNull JavaPairRDD<Long, BitSet> bitmapOfSecondDimension) {
        JavaPairRDD<Rankings, BitSlices> rankingsWithCurrentBitSlices
                = getRankingsWithBitSlices(pointsWithRankings, bitmapOfFirstDimension, bitmapOfSecondDimension);

        JavaPairRDD<Rankings, BitSlices> rankingsWithPreviousBitSlices
                = getRankingsWithBitSlices(pointsWithPreviousRankings, bitmapOfFirstDimension, bitmapOfSecondDimension);

        JavaPairRDD<Rankings, Tuple2<BitSlices, BitSlices>> joined
                = rankingsWithCurrentBitSlices
                .join(rankingsWithPreviousBitSlices)
                .mapToPair(p -> new Tuple2<>(p._1(), new Tuple2<>(p._2()._1(), p._2()._2())));


        return pointsWithRankings
                .join(joined)
                .map(p -> PointsWithRequiredBitSlices.fromTuple(p._2()));
    }

    private JavaPairRDD<Rankings, BitSlices> getRankingsWithBitSlices(JavaPairRDD<Rankings, Point2D> rankings,
                                                                      JavaPairRDD<Long, BitSet> bitmapOfFirstDimension,
                                                                      JavaPairRDD<Long, BitSet> bitmapOfSecondDimension) {
        JavaPairRDD<Rankings, BitSet> withFirstDimensionBitSlices
                = rankings
                .map(Tuple2::_1)
                .keyBy(Rankings::getFirstDimensionRanking)
                .join(bitmapOfFirstDimension)
                .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2()));

        JavaPairRDD<Rankings, BitSet> withSecondDimensionBitSlices
                = rankings
                .map(Tuple2::_1)
                .keyBy(Rankings::getSecondDimensionRanking)
                .join(bitmapOfSecondDimension)
                .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2()));

        return withFirstDimensionBitSlices
                .join(withSecondDimensionBitSlices)
                .mapToPair(p -> new Tuple2<>(p._1(), BitSlices.fromTuple(p._2())));
    }
}
