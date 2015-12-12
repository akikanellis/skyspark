package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import javax.validation.constraints.NotNull;
import java.util.BitSet;
import java.util.List;

class BitmapStructure {

    private final JavaRDD<Double> dimensionValues;
    private final int numberOfPartitions;

    private JavaRDD<BitSet> dimensionBitmap;

    BitmapStructure(@NotNull JavaRDD<Double> dimensionValues, final int numberOfPartitions) {
        this.dimensionValues = dimensionValues;
        this.numberOfPartitions = numberOfPartitions;
    }

    public void /* TODO change */ create() {
        JavaRDD<Double> distinctSortedPoints = getDistinctSorted();

        JavaPairRDD<Double, Long> indexed = mapWithIndex(distinctSortedPoints);

        JavaRDD<BitSet> bitSets = calculateBitSets(indexed);
    }

    JavaRDD<BitSet> calculateBitSets(JavaPairRDD<Double, Long> indexed) {

        return dimensionValues.map(v -> {
            BitSet bitSet = new BitSet();
            List<Long> ls = indexed.lookup(5.4); // can't call lookup inside .map
            final long lastIndex = indexed.lookup(v).get(0);
            bitSet.set(0, (int) lastIndex - 1);

            return bitSet;
        });
    }

    JavaPairRDD<Double, Long> mapWithIndex(JavaRDD<Double> distinctPointsOfDimension) {
        return distinctPointsOfDimension.zipWithIndex();
    }

    // We use ascending order because our points dominate each other when they are less in every dimension.
    JavaRDD<Double> getDistinctSorted() {
        return dimensionValues
                .distinct()
                .sortBy(Double::doubleValue, true, numberOfPartitions);
    }
}
