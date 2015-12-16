package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.bitmap.BitSlice;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.BitSet;

public interface FullBitmapStructureMock {
    JavaRDD<Double> getDimensionValues();

    JavaRDD<Double> getDistinctValuesSorted();

    JavaPairRDD<Double, Long> getValuesIndexed();

    JavaRDD<BitSet> getValuesBitSets();

    JavaRDD<BitSlice> getValuesBitSlices();
}