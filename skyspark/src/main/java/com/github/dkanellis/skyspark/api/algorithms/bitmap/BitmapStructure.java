package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.utils.BitSets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.BitSet;

class BitmapStructure implements Serializable {

    private final JavaRDD<Double> dimensionValues;
    private final int numberOfPartitions;
    private long uniqueDimensionValuesSize;

    private JavaRDD<BitSet> dimensionBitmap;
    private JavaRDD<BitSlice> bitSlices;

    BitmapStructure(@NotNull JavaRDD<Double> dimensionValues, final int numberOfPartitions) {
        this.dimensionValues = dimensionValues;
        this.numberOfPartitions = numberOfPartitions;
    }

    public void /* TODO change */ create() {
        JavaRDD<Double> distinctSortedPoints = getDistinctSorted();
        uniqueDimensionValuesSize = distinctSortedPoints.count();

        JavaPairRDD<Double, Long> indexed = mapWithIndex(distinctSortedPoints);

        JavaRDD<BitSet> bitSets = calculateBitSets(indexed, uniqueDimensionValuesSize);

        bitSlices = calculateBitSlices(indexed, bitSets);
    }

    JavaRDD<BitSlice> calculateBitSlices(JavaPairRDD<Double, Long> indexed, JavaRDD<BitSet> bitSets) {
        return indexed
                .cartesian(bitSets)
                .groupByKey()
                .map(BitSlice::fromTuple);
    }

    JavaRDD<BitSet> calculateBitSets(JavaPairRDD<Double, Long> indexed, long uniqueDimensionValuesSize) {
        JavaPairRDD<Double, Tuple2<Double, Long>> combinations = dimensionValues.cartesian(indexed);
        JavaPairRDD<Double, Long> dimensionValuesWithRanking = combinations
                .filter(v -> v._1().equals(v._2()._1()))
                .mapToPair(v -> new Tuple2<>(v._1(), v._2()._2()));

        return dimensionValuesWithRanking
                .map(v -> BitSets.bitSetFromIndexes(0, v._2()));
    }

    JavaPairRDD<Double, Long> mapWithIndex(JavaRDD<Double> distinctPointsOfDimension) {
        return distinctPointsOfDimension.zipWithIndex();
    }

    // We use ascending order because our points dominate each other when they are less in every dimension.
    JavaRDD<Double> getDistinctSorted() {
        return dimensionValues
                .distinct()
                .sortBy(Double::doubleValue, false, numberOfPartitions);
    }
}
