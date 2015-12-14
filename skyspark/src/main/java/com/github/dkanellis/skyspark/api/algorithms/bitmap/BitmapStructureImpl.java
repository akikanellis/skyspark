package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.utils.BitSets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.util.BitSet;

import static com.google.common.base.Preconditions.checkNotNull;


class BitmapStructureImpl implements BitmapStructure {

    private final int numberOfPartitions;
    private final BitSliceCreator bitSliceCreator;
    private JavaRDD<BitSlice> bitSlices;

    BitmapStructureImpl(final int numberOfPartitions, @NotNull BitSliceCreator bitSliceCreator) {
        this.numberOfPartitions = numberOfPartitions;
        this.bitSliceCreator = checkNotNull(bitSliceCreator);
    }

    @Override
    public void init(@NotNull JavaRDD<Double> dimensionValues) {
        checkNotNull(dimensionValues);

        JavaPairRDD<Double, Long> distinctSortedPointsWithIndex = getDistinctSortedWithIndex(dimensionValues);

        JavaRDD<BitSet> bitSets = calculateBitSets(dimensionValues, distinctSortedPointsWithIndex);

        bitSlices = calculateBitSlices(distinctSortedPointsWithIndex, bitSets);
    }

    @Override
    public BitSet getCorrespondingBitSlice(final double dimensionValue) {
        return bitSlices
                .filter(bs -> bs.getDimensionValue() == dimensionValue)
                .first()
                .getBitVector();
    }

    @Override
    public JavaRDD<BitSlice> rdd() {
        return bitSlices;
    }

    JavaPairRDD<Double, Long> getDistinctSortedWithIndex(JavaRDD<Double> dimensionValues) {
        return dimensionValues
                .distinct()
                .sortBy(Double::doubleValue, false, numberOfPartitions)
                .zipWithIndex();
    }

    JavaRDD<BitSet> calculateBitSets(JavaRDD<Double> dimensionValues, JavaPairRDD<Double, Long> indexed) {
        JavaPairRDD<Double, Tuple2<Double, Long>> combinations = dimensionValues.cartesian(indexed);
        JavaPairRDD<Double, Long> dimensionValuesWithRanking = combinations
                .filter(v -> v._1().equals(v._2()._1()))
                .mapToPair(v -> new Tuple2<>(v._1(), v._2()._2()));

        return dimensionValuesWithRanking
                .map(v -> BitSets.bitSetFromIndexes(0, v._2() + 1));
    }

    JavaRDD<BitSlice> calculateBitSlices(JavaPairRDD<Double, Long> indexed, JavaRDD<BitSet> bitSets) {
        return indexed
                .cartesian(bitSets)
                .groupByKey()
                .map(bitSliceCreator::from);
    }
}