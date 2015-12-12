package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.BitSet;

public interface FullBitmapStructureMock {
    JavaRDD<Double> getDistinctValuesSorted();

    JavaPairRDD<Long, Double> getValuesIndexed();

    JavaRDD<BitSet> getValuesBitSets();
}
