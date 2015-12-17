package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.utils.BitSets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.util.BitSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


class BitmapStructureImpl implements BitmapStructure {

    private final int numberOfPartitions;
    private final BitSliceCreator bitSliceCreator;
    private final JavaPairRDD<Long, BitSet> defaultValueRdd;

    BitmapStructureImpl(final int numberOfPartitions, @NotNull BitSliceCreator bitSliceCreator,
                        JavaPairRDD<Long, BitSet> defaultValueRdd) {
        this.numberOfPartitions = numberOfPartitions;
        this.bitSliceCreator = checkNotNull(bitSliceCreator);
        this.defaultValueRdd = defaultValueRdd;
    }

    @Override
    public JavaPairRDD<Long, BitSet> computeBitSlices(@NotNull JavaRDD<Double> dimensionValues,
                                                      @NotNull JavaPairRDD<Double, Long> distinctValuesWithRankings) {
        checkNotNull(dimensionValues);

        Long sizeOfUniqueValues = distinctValuesWithRankings.count();

        JavaRDD<BitSet> bitSets = calculateBitSets(dimensionValues, distinctValuesWithRankings, sizeOfUniqueValues);

        return calculateBitSlices(distinctValuesWithRankings, bitSets, sizeOfUniqueValues);
    }

    JavaRDD<BitSet> calculateBitSets(JavaRDD<Double> dimensionValues, JavaPairRDD<Double, Long> indexed, Long sizeOfUniqueValues) {
        return dimensionValues
                .zipWithIndex()
                .join(indexed)
                .mapToPair(v -> new Tuple2<>(BitSets.bitSetFromIndexes(0, sizeOfUniqueValues - v._2()._2()), v._2()._1()))
                .map(Tuple2::swap)
                .sortBy(Tuple2::_1, true, numberOfPartitions)
                .map(Tuple2::_2);
    }

    JavaPairRDD<Long, BitSet> calculateBitSlices(JavaPairRDD<Double, Long> indexed, JavaRDD<BitSet> bitSets, Long sizeOfUniqueValues) {
        JavaRDD<List<BitSet>> glomed = bitSets
                .coalesce(1)
                .glom();

        return glomed
                .cartesian(indexed)
                .mapToPair(p -> bitSliceCreator.from(p, sizeOfUniqueValues))
                .union(defaultValueRdd);
    }
}