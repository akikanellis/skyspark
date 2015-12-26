package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.utils.point.DominationComparatorMinAnotation;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public class Bitmap implements SkylineAlgorithm {

    //    private final RankingCalculator rankingCalculator;
//    private final PointsWithBitmapMerger pointsWithBitmapMerger;
    private BitmapCalculator bitmapCalculator;
    private int numberOfPartitions;
    private int numberOfDimensions = 2;

    public Bitmap() {
        this(4);
    }

    public Bitmap(final int numberOfPartitions) {
        checkArgument(numberOfPartitions > 0, "Partitions can't be less than 1.");

        this.numberOfPartitions = numberOfPartitions;
//        this.rankingCalculator = new RankingCalculatorImpl(numberOfPartitions);
//        this.pointsWithBitmapMerger = new PointsWithBitmapMergerImpl();
    }

    @Override
    public JavaRDD<Point2D> computeSkylinePoints(JavaRDD<Point2D> points) {
        bitmapCalculator
                = new BitmapCalculator(numberOfPartitions, (JavaSparkContext.fromSparkContext(points.context())));


        // ((dim, value), key)
        JavaPairRDD<Tuple2<Integer, Double>, Point2D> dimensionValuePoint = points
                .flatMapToPair(p -> {
                    List<Tuple2<Tuple2<Integer, Double>, Point2D>> splitByDimension = new ArrayList<>();
                    splitByDimension.add(new Tuple2<>(new Tuple2<>(1, p.getX()), p));
                    splitByDimension.add(new Tuple2<>(new Tuple2<>(2, p.getY()), p));

                    return splitByDimension;
                });

        // (dim, list(value))
        JavaPairRDD<Integer, Iterable<Double>> grouped = dimensionValuePoint
                .mapToPair(p -> new Tuple2<>(p._1()._1(), p._1()._2()))
                .groupByKey();

        /*
        foreach dimension
        produce Dimension, map<Value, Ranking>
         */

        // (dim, value), ranking
        JavaPairRDD<Tuple2<Integer, Double>, Integer> dimensionValueRanking = grouped.flatMapToPair(p -> {
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

//        points.context().broadcast(dimensionValueRanking.collectAsMap(), Map.class)


        // (dim, value), list(point, ranking)

        JavaPairRDD<Point2D, List<Tuple2<Integer, Integer>>> flattenedWithRankings = dimensionValuePoint.join(dimensionValueRanking)
                .mapToPair(p -> new Tuple2<>(p._2()._1(), new Tuple2<>(p._1()._1(), p._2()._2())))
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1(), Lists.newArrayList(p._2())));

        final Map<Tuple2<Integer, Integer>, BitSet> bitmap = bitmapCalculator.computeBitSlices(points).reduce((a, b) -> {
            a.putAll(b);
            return a;
        });

        Broadcast<Map<Tuple2<Integer, Integer>, BitSet>> broadcastedBitmap = JavaSparkContext.fromSparkContext(points.context()).broadcast(bitmap);

        JavaRDD<Point2D> skylines = flattenedWithRankings.filter(p -> {
            List<Tuple2<Integer, Integer>> dimensionsWithRanks = p._2();
            Tuple2<Integer, Integer> positionInBitmap = dimensionsWithRanks.get(0);
            BitSet A = bitSlice(positionInBitmap._2(), positionInBitmap._1(), broadcastedBitmap.value());
            for (int i = 2; i <= numberOfDimensions; ++i) {
                positionInBitmap = dimensionsWithRanks.get(i - 1);
                A.and(bitSlice(positionInBitmap._2(), positionInBitmap._1(), broadcastedBitmap.value()));
            }


            positionInBitmap = dimensionsWithRanks.get(0);
            BitSet B = bitSlice(positionInBitmap._2() - 1, positionInBitmap._1(), broadcastedBitmap.value());
            for (int i = 2; i <= numberOfDimensions; ++i) {
                positionInBitmap = dimensionsWithRanks.get(i - 1);
                B.or(bitSlice(positionInBitmap._2() - 1, positionInBitmap._1(), broadcastedBitmap.value()));
            }

            BitSet C = A;
            C.and(B);

            return C.isEmpty();
        }).map(Tuple2::_1);

        return skylines;
    }

    BitSet bitSlice(final int rank, final int dimension, final Map<Tuple2<Integer, Integer>, BitSet> bitmap) {
        if (rank < 0) {
            return new BitSet();
        }

        Tuple2<Integer, Integer> key = new Tuple2<>(dimension, rank);

        return (BitSet) bitmap.get(key).clone();
    }

    @Override
    public String toString() {
        return "Bitmap";
    }
}
