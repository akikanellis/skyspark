package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class Bitmap implements SkylineAlgorithm, Serializable {

    private final BitmapStructure bitmapCalculator;
    private final RankingCalculator rankingCalculator;
    private final PointsWithBitmapMerger pointsWithBitmapMerger;

    public Bitmap(SparkContextWrapper sparkContextWrapper) {
        this(sparkContextWrapper, 4);
    }

    public Bitmap(SparkContextWrapper sparkContextWrapper, final int numberOfPartitions) {
        checkArgument(numberOfPartitions > 0, "Partitions can't be less than 1.");

        this.bitmapCalculator = Injector.getBitmapStructure(sparkContextWrapper, numberOfPartitions);
        this.rankingCalculator = new RankingCalculatorImpl(numberOfPartitions);
        this.pointsWithBitmapMerger = new PointsWithBitmapMergerImpl();
    }

    @Override
    public List<Point2D> getSkylinePoints(JavaRDD<Point2D> points) {
        JavaRDD<Double> firstDimensionValues = points.map(Point2D::getX);
        JavaRDD<Double> secondDimensionValues = points.map(Point2D::getX);

        JavaPairRDD<Double, Long> firstDimensionWithRankings = rankingCalculator.computeDistinctRankings(firstDimensionValues);
        JavaPairRDD<Double, Long> secondDimensionWithRankings = rankingCalculator.computeDistinctRankings(secondDimensionValues);

        JavaPairRDD<Rankings, Point2D> pointsWithRankings
                = rankingCalculator.applyRankingsToAllPoints(points, firstDimensionWithRankings, secondDimensionWithRankings);
        JavaPairRDD<Rankings, Point2D> pointsWithPreviousRankings
                = rankingCalculator.getWithPreviousRankingsPoints(pointsWithRankings);

        JavaPairRDD<Long, BitSet> bitmapOfFirstDimension
                = bitmapCalculator.computeBitSlices(firstDimensionValues, firstDimensionWithRankings);
        JavaPairRDD<Long, BitSet> bitmapOfSecondDimension
                = bitmapCalculator.computeBitSlices(secondDimensionValues, secondDimensionWithRankings);

        JavaRDD<PointsWithRequiredBitSlices> pointsWithRequiredBitSlices
                = pointsWithBitmapMerger.mergePointsWithBitmap(pointsWithRankings, pointsWithPreviousRankings,
                bitmapOfFirstDimension, bitmapOfSecondDimension);

        JavaRDD<Point2D> skylines = pointsWithRequiredBitSlices
                .filter(PointsWithRequiredBitSlices::isSkyline)
                .map(PointsWithRequiredBitSlices::getPoint);

        return skylines.collect();
    }

    @Override
    public String toString() {
        return "Bitmap";
    }
}
