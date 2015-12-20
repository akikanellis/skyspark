package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import javax.validation.constraints.NotNull;
import java.awt.geom.Point2D;
import java.util.BitSet;

interface PointsWithBitmapMerger {

    JavaRDD<PointsWithRequiredBitSlices> mergePointsWithBitmap(@NotNull JavaPairRDD<Rankings, Point2D> pointsWithRankings,
                                                               @NotNull JavaPairRDD<Rankings, Point2D> pointsWithPreviousRankings,
                                                               @NotNull JavaPairRDD<Long, BitSet> bitmapOfFirstDimension,
                                                               @NotNull JavaPairRDD<Long, BitSet> bitmapOfSecondDimension);
}
