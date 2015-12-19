package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.BitSet;

public interface FullBitmapStructureMock {

    Long getSizeOfUniqueValues();

    JavaRDD<Double> getDimensionValues();

    JavaPairRDD<Double, Long> getValuesIndexed();

    JavaRDD<BitSet> getValuesBitSets();

    JavaPairRDD<Long, BitSet> getValuesBitSlices();
}
