package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.BitSet;

import static com.github.dkanellis.skyspark.api.utils.BitSets.bitSetfromString;

public class FullDimensionYBitmapStructureMock implements FullBitmapStructureMock {

    private final SparkContextWrapper sparkContextWrapper;

    public FullDimensionYBitmapStructureMock(SparkContextWrapper sparkContextWrapper) {
        this.sparkContextWrapper = sparkContextWrapper;
    }

    @Override
    public JavaRDD<Double> getDistinctValuesSorted() {
        return sparkContextWrapper.parallelize(Arrays.asList(3.3, 3.4, 3.5, 4.0, 4.1, 4.4, 4.6, 7.3, 9.0));
    }

    @Override
    public JavaPairRDD<Long, Double> getValuesIndexed() {
        return sparkContextWrapper.parallelizePairs(Arrays.asList(
                new Tuple2<>(0L, 3.3),
                new Tuple2<>(1L, 3.4),
                new Tuple2<>(2L, 3.5),
                new Tuple2<>(3L, 4.0),
                new Tuple2<>(4L, 4.1),
                new Tuple2<>(5L, 4.4),
                new Tuple2<>(6L, 4.6),
                new Tuple2<>(7L, 7.3),
                new Tuple2<>(8L, 9.0)
        ));
    }

    @Override
    public JavaRDD<BitSet> getValuesBitSets() {
        return sparkContextWrapper.parallelize(Arrays.asList(
                bitSetfromString("000001111"), // 4.4
                bitSetfromString("000011111"), // 4.1
                bitSetfromString("000000001"), // 9.0
                bitSetfromString("000111111"), // 4.0
                bitSetfromString("000000111"), // 4.6
                bitSetfromString("000000011"), // 7.3
                bitSetfromString("001111111"), // 3.5
                bitSetfromString("000011111"), // 4.1
                bitSetfromString("111111111"), // 3.3
                bitSetfromString("011111111")  // 3.4
        ));
    }
}
