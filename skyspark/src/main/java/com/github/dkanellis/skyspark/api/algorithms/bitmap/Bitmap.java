package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.awt.geom.Point2D;
import java.util.BitSet;

import static com.google.common.base.Preconditions.checkArgument;

public class Bitmap implements SkylineAlgorithm {

    private final RankingCalculator rankingCalculator;
    private final PointsWithBitmapMerger pointsWithBitmapMerger;
    private BitmapStructure bitmapCalculator;
    private int numberOfPartitions;

    public Bitmap() {
        this(4);
    }

    public Bitmap(final int numberOfPartitions) {
        checkArgument(numberOfPartitions > 0, "Partitions can't be less than 1.");

        this.numberOfPartitions = numberOfPartitions;
        this.rankingCalculator = new RankingCalculatorImpl(numberOfPartitions);
        this.pointsWithBitmapMerger = new PointsWithBitmapMergerImpl();
    }

    @Override
    public JavaRDD<Point2D> computeSkylinePoints(JavaRDD<Point2D> points) {
        bitmapCalculator
                = new BitmapStructureImpl(numberOfPartitions, (JavaSparkContext.fromSparkContext(points.context())));

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

        return skylines;
    }

    @Override
    public String toString() {
        return "Bitmap";
    }
}
