package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.utils.BitSets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


class BitmapStructureImpl implements BitmapStructure {

    private final int numberOfPartitions;
    private final BitSliceCreator bitSliceCreator;
    private final JavaPairRDD<Long, BitSet> defaultValueRdd;

    public BitmapStructureImpl(final int numberOfPartitions, JavaSparkContext sparkContext) {
        this.numberOfPartitions = numberOfPartitions;
        this.defaultValueRdd = getDefaultValueRdd(sparkContext);
        this.bitSliceCreator = new BitSliceCreatorImpl();
    }

    private JavaPairRDD<Long, BitSet> getDefaultValueRdd(JavaSparkContext sparkContextWrapper) {
        Tuple2<Long, BitSet> defaultValue = BitSliceCreator.DEFAULT;
        List<Tuple2<Long, BitSet>> edgeCaseList = Collections.singletonList(defaultValue);
        JavaRDD<Tuple2<Long, BitSet>> rdd = sparkContextWrapper.parallelize(edgeCaseList);

        return rdd.mapToPair(p -> new Tuple2<>(p._1(), p._2()));
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