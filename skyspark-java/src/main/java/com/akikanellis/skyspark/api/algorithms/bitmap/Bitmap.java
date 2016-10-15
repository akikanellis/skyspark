package com.akikanellis.skyspark.api.algorithms.bitmap;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.api.utils.point.DominationComparatorMinAnotation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.*;

import static org.apache.commons.math3.util.MathUtils.checkNotNull;

/**
 * Bitmap is a skyline algorithm which maps every unique point of each dimension to a bitmap, maps each point with its
 * list of bitmap keys and uses in the end the bitmap algorithm to see if a point is a skyline or not.
 */
public class Bitmap implements SkylineAlgorithm {

    /**
     * Used for mapping each point to it's appropriate keys needed from the bitmap.
     */
    private final PointToRankMapper pointToRankMapper;

    /**
     * Used to calculate the bitmap.
     */
    private final BitmapCalculator bitmapCalculator;

    /**
     * The number of dimensions of the dataset.
     */
    private final int numberOfDimensions;

    /**
     * Data structure containing the bitmap as well as bitmap accessing functions.
     */
    private BitmapStructure bitmapStructure;

    public Bitmap() {
        numberOfDimensions = 2;
        bitmapCalculator = new BitmapCalculator();
        pointToRankMapper = new PointToRankMapper();
    }

    /*
     * In Bitmap first we separate the dimensions from the values, group them together, add their ranks
     * (which is biggest/smallest), map the ranking to every point and thus we end up with every point being mapped to
     * a list of keys needed for accessing the bitmap. Then we create the bitmap structure and we use the bitmap
     * algorithm to check if a point belongs to the skyline.
     */
    @Override
    public JavaRDD<Point2D> computeSkylinePoints(JavaRDD<Point2D> points) {
        checkNotNull(points);

        JavaPairRDD<Tuple2<Integer, Double>, Point2D> dimensionValuePoint = splitPointsToDimensionsWithValues(points);

        JavaPairRDD<Integer, Iterable<Double>> groupedByDimension = dimensionValuePoint
                .mapToPair(p -> new Tuple2<>(p._1()._1(), p._1()._2()))
                .groupByKey();

        JavaPairRDD<Tuple2<Integer, Double>, Integer> dimensionWithValueRanking = getDimensionWithValuesAndRankings(groupedByDimension);

        JavaPairRDD<Point2D, List<Tuple2<Integer, Integer>>> flattenedWithRankings
                = pointToRankMapper.mapPointsToTheirRankings(dimensionValuePoint, dimensionWithValueRanking);

        JavaPairRDD<Tuple2<Integer, Integer>, BitSet> bitmap = bitmapCalculator.computeBitSlices(dimensionWithValueRanking, groupedByDimension);
        bitmapStructure = new BitmapStructure(bitmap);

        JavaRDD<Point2D> skylines = flattenedWithRankings
                .filter(p -> isSkyline(p._2()))
                .map(Tuple2::_1);

        return skylines;
    }

    private JavaPairRDD<Tuple2<Integer, Double>, Point2D> splitPointsToDimensionsWithValues(JavaRDD<Point2D> points) {
        return points
                .flatMapToPair(p -> {
                    List<Tuple2<Tuple2<Integer, Double>, Point2D>> splitByDimension = new ArrayList<>();
                    splitByDimension.add(new Tuple2<>(new Tuple2<>(1, p.getX()), p));
                    splitByDimension.add(new Tuple2<>(new Tuple2<>(2, p.getY()), p));

                    return splitByDimension;
                });
    }

    private JavaPairRDD<Tuple2<Integer, Double>, Integer> getDimensionWithValuesAndRankings(JavaPairRDD<Integer, Iterable<Double>> grouped) {
        return grouped.flatMapToPair(p -> {
            final int currentDimension = p._1();
            Iterable<Double> allDimensionValues = p._2();

            SortedSet<Double> distinctSorted = new TreeSet<>(new DominationComparatorMinAnotation());
            allDimensionValues.forEach(distinctSorted::add);

            List<Tuple2<Tuple2<Integer, Double>, Integer>> ranked = new ArrayList<>();
            int index = 0;
            for (Double distinctValue : distinctSorted) {
                Tuple2<Integer, Double> key = new Tuple2<>(currentDimension, distinctValue);
                ranked.add(new Tuple2<>(key, index++));
            }

            return ranked;
        });
    }

    private boolean isSkyline(List<Tuple2<Integer, Integer>> dimensionsWithRanks) {
        Tuple2<Integer, Integer> positionInBitmap = dimensionsWithRanks.get(0);
        BitSet A = bitmapStructure.getBitSlice(positionInBitmap._1(), positionInBitmap._2());
        for (int i = 2; i <= numberOfDimensions; ++i) {
            positionInBitmap = dimensionsWithRanks.get(i - 1);
            A.and(bitmapStructure.getBitSlice(positionInBitmap._1(), positionInBitmap._2()));
        }


        positionInBitmap = dimensionsWithRanks.get(0);
        BitSet B = bitmapStructure.getBitSlice(positionInBitmap._1(), positionInBitmap._2() - 1);
        for (int i = 2; i <= numberOfDimensions; ++i) {
            positionInBitmap = dimensionsWithRanks.get(i - 1);
            B.or(bitmapStructure.getBitSlice(positionInBitmap._1(), positionInBitmap._2() - 1));
        }

        BitSet C = A;
        C.and(B);

        return C.isEmpty();
    }

    @Override
    public String toString() {
        return "Bitmap";
    }
}
