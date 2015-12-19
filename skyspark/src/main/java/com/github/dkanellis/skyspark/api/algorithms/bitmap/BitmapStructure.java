package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.BitSet;

public interface BitmapStructure extends Serializable {
    JavaPairRDD<Long, BitSet> computeBitSlices(@NotNull JavaRDD<Double> dimensionValues,
                                               @NotNull JavaPairRDD<Double, Long> distinctValuesWithRankings);
}
