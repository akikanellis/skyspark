package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.utils.SerializedLists;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.Tuple3;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

/**
 * This class produces the bit slices for the Bitmap algorithm. To maximise reusability and performance we do not use the
 * point RDD directly but it's dimension with value ranking and the groupings for each dimension.
 */
class BitmapCalculator implements Serializable {

    /**
     * Compute the bit slices.
     *
     * @param dimensionValueRanking each dimension-value key mapped to it's respective ranking.
     * @param groupedByDimension    each dimension mapped to it's list of values.
     * @return a tuple of <<Dimension, Ranking>, BitSlice> which are the bit slices for each dimension.
     */
    public JavaPairRDD<Tuple2<Integer, Integer>, BitSet> computeBitSlices(
            @NotNull JavaPairRDD<Tuple2<Integer, Double>, Integer> dimensionValueRanking,
            @NotNull JavaPairRDD<Integer, Iterable<Double>> groupedByDimension) {

        JavaPairRDD<Tuple3<Integer, Double, Integer>, List<Double>> rankedWithAllValues
                = getRankedWithAllValues(dimensionValueRanking, groupedByDimension);

        JavaPairRDD<Tuple2<Integer, Integer>, BitSet> bitSlices = rankedWithAllValues.mapToPair(r -> {
            final int currentDimension = r._1()._1();
            Double currentRankedValue = r._1()._2();
            final int currentRank = r._1()._3();
            List<Double> allDimensionValuesReversed = r._2();

            BitSet bitSlice = new BitSet();
            int correspondingBitIndex = 0;
            for (Double value : allDimensionValuesReversed) {
                if (currentRankedValue >= value) {
                    bitSlice.set(correspondingBitIndex);
                }
                ++correspondingBitIndex;
            }

            Tuple2<Integer, Integer> dimensionWithRank = new Tuple2<>(currentDimension, currentRank);

            return new Tuple2<>(dimensionWithRank, bitSlice);
        });

        return bitSlices;
    }

    private JavaPairRDD<Tuple3<Integer, Double, Integer>, List<Double>> getRankedWithAllValues(
            @NotNull JavaPairRDD<Tuple2<Integer, Double>, Integer> dimensionValueRanking,
            @NotNull JavaPairRDD<Integer, Iterable<Double>> groupedFromOther) {

        JavaPairRDD<Integer, Tuple2<Double, Integer>> dimensionValueRankingRemapped
                = dimensionValueRanking.mapToPair(p -> new Tuple2<>(p._1()._1(), new Tuple2<>(p._1()._2(), p._2())));

        JavaPairRDD<Integer, List<Double>> allValuesGroupedByDimensionAndReversed = groupedFromOther
                .mapToPair(p -> new Tuple2<>(p._1(), SerializedLists.reverseAndKeepSerialized(p._2())));

        JavaPairRDD<Integer, Tuple2<Tuple2<Double, Integer>, List<Double>>> joined
                = dimensionValueRankingRemapped.join(allValuesGroupedByDimensionAndReversed);

        return joined.mapToPair(p -> {
            final int dimension = p._1();
            final double value = p._2()._1()._1();
            final int rank = p._2()._1()._2();
            List<Double> allValues = p._2()._2();

            Tuple3<Integer, Double, Integer> key = new Tuple3<>(dimension, value, rank);

            return new Tuple2<>(key, allValues);
        });
    }
}