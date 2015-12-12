package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.BitSet;

import static com.github.dkanellis.skyspark.api.utils.BitSets.bitSetfromString;

public class FullDimensionXBitmapStructureMock implements FullBitmapStructureMock {

    private final SparkContextWrapper sparkContextWrapper;

    public FullDimensionXBitmapStructureMock(SparkContextWrapper sparkContextWrapper) {
        this.sparkContextWrapper = sparkContextWrapper;
    }

    @Override
    public JavaRDD<Double> getDistinctValuesSorted() {
        return sparkContextWrapper.parallelize(Arrays.asList(2.5, 3.6, 5.0, 5.4, 5.9, 6.1, 6.3, 6.7, 9.9));
    }

    @Override
    public JavaPairRDD<Long, Double> getValuesIndexed() {
        return sparkContextWrapper.parallelizePairs(Arrays.asList(
                new Tuple2<>(0L, 2.5),
                new Tuple2<>(1L, 3.6),
                new Tuple2<>(2L, 5.0),
                new Tuple2<>(3L, 5.4),
                new Tuple2<>(4L, 5.9),
                new Tuple2<>(5L, 6.1),
                new Tuple2<>(6L, 6.3),
                new Tuple2<>(7L, 6.7),
                new Tuple2<>(8L, 9.9)
        ));
    }

    @Override
    public JavaRDD<BitSet> getValuesBitSets() {
        return sparkContextWrapper.parallelize(Arrays.asList(
                bitSetfromString("000111111"), // 5.4
                bitSetfromString("001111111"), // 5.0
                bitSetfromString("011111111"), // 3.6
                bitSetfromString("000011111"), // 5.9
                bitSetfromString("000011111"), // 5.9
                bitSetfromString("111111111"), // 2.5
                bitSetfromString("000000111"), // 6.3
                bitSetfromString("000000001"), // 9.9
                bitSetfromString("000000011"), // 6.7
                bitSetfromString("000001111")  // 6.1
        ));
    }
}
