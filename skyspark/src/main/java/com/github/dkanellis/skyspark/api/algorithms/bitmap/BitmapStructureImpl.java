package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.utils.BitSets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import javax.validation.constraints.NotNull;
import java.util.BitSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


class BitmapStructureImpl implements BitmapStructure {

    private final int numberOfPartitions;
    private final BitSliceCreator bitSliceCreator;
    private final JavaPairRDD<Long, BitSlice> defaultValueRdd;
    Long sizeOfUniqueValues;
    private JavaPairRDD<Long, BitSlice> bitSlices;
    private JavaPairRDD<Double, Long> distinctSortedPointsWithIndex;

    BitmapStructureImpl(final int numberOfPartitions, @NotNull BitSliceCreator bitSliceCreator,
                        JavaPairRDD<Long, BitSlice> defaultValueRdd) {
        this.numberOfPartitions = numberOfPartitions;
        this.bitSliceCreator = checkNotNull(bitSliceCreator);
        this.defaultValueRdd = defaultValueRdd;
    }

    @Override
    public void init(@NotNull JavaRDD<Double> dimensionValues) {
        checkNotNull(dimensionValues);

        distinctSortedPointsWithIndex = getDistinctSortedWithIndex(dimensionValues);

        sizeOfUniqueValues = distinctSortedPointsWithIndex.count();

        JavaRDD<BitSet> bitSets = calculateBitSets(dimensionValues, distinctSortedPointsWithIndex, sizeOfUniqueValues);

        bitSlices = calculateBitSlices(distinctSortedPointsWithIndex, bitSets, sizeOfUniqueValues);
    }

    @Override
    public JavaPairRDD<Double, Long> rankingsRdd() {
        return distinctSortedPointsWithIndex;
    }

    @Override
    public JavaPairRDD<Long, BitSlice> bitSlicesRdd() {
        return bitSlices;
    }

    JavaPairRDD<Double, Long> getDistinctSortedWithIndex(JavaRDD<Double> dimensionValues) {
        return dimensionValues
                .distinct()
                .sortBy(Double::doubleValue, true, numberOfPartitions)
                .zipWithIndex();
    }

    JavaRDD<BitSet> calculateBitSets(JavaRDD<Double> dimensionValues, JavaPairRDD<Double, Long> indexed, Long sizeOfUniqueValues) {
        return dimensionValues
                .keyBy(Double::doubleValue)
                .join(indexed)
                .map(v -> BitSets.bitSetFromIndexes(0, sizeOfUniqueValues - v._2()._2()));
    }

    JavaPairRDD<Long, BitSlice> calculateBitSlices(JavaPairRDD<Double, Long> indexed, JavaRDD<BitSet> bitSets, Long sizeOfUniqueValues) {
        JavaRDD<List<BitSet>> glomed = bitSets
                .coalesce(1)
                .glom();

        JavaPairRDD<Long, BitSlice> result = glomed
                .cartesian(indexed)
                .mapToPair(p -> bitSliceCreator.from(p, sizeOfUniqueValues))
                .union(defaultValueRdd);

        result.takeSample(true, 10)
                .forEach(p -> System.out.printf("(value=%f, index=%d, bits=%d)\n",
                        p._2().getDimensionValue(), p._1(), p._2().getBitVector().size()));

        return result;
    }
}