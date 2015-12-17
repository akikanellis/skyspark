package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.BitSet;

public interface BitmapStructure extends Serializable {
    void init(@NotNull JavaRDD<Double> dimensionValues);

    JavaPairRDD<Double, Long> rankingsRdd();

    JavaPairRDD<Long, BitSet> bitSlicesRdd();
}
