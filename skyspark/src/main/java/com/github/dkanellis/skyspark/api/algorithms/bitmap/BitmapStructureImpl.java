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

        JavaPairRDD<Double, BitSet> bitSets = calculateBitSets(dimensionValues, distinctSortedPointsWithIndex);

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

    JavaPairRDD<Double, BitSet> calculateBitSets(JavaRDD<Double> dimensionValues, JavaPairRDD<Double, Long> indexed) {
        return dimensionValues
                .keyBy(Double::doubleValue)
                .join(indexed)
                .mapToPair(v -> new Tuple2<>(v._1(), BitSets.bitSetFromIndexes(0, v._2()._2() + 1)));
    }

    JavaRDD<BitSlice> calculateBitSlices(JavaPairRDD<Double, Long> indexed, JavaPairRDD<Double, BitSet> bitSets) {
        System.out.println("Size before: " + in)
        indexed
                .join(bitSets)
                .groupByKey()
                .take(10).forEach(p -> System.out.printf("(value=%f, index=%d, bits=%d)\n", p., p._2()._1(), p._2()._2().size()));
//        return indexed
//                .join(bitSets)
//                .;
        return null;
    }
}