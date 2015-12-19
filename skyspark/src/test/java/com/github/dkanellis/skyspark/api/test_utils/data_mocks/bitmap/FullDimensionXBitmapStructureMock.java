package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.BitSet;

import static com.github.dkanellis.skyspark.api.utils.BitSets.bitSetFromString;

public class FullDimensionXBitmapStructureMock implements FullBitmapStructureMock {

    private final SparkContextWrapper sparkContextWrapper;

    public FullDimensionXBitmapStructureMock(SparkContextWrapper sparkContextWrapper) {
        this.sparkContextWrapper = sparkContextWrapper;
    }

    @Override
    public Long getSizeOfUniqueValues() {
        return 9L;
    }

    @Override
    public JavaRDD<Double> getDimensionValues() {
        return BitmapPointsMock.get10Points(sparkContextWrapper).map(Point2D::getX);
    }

    @Override
    public JavaPairRDD<Double, Long> getValuesIndexed() {
        return sparkContextWrapper.parallelizePairs(Arrays.asList(
                new Tuple2<>(2.5, 0L),
                new Tuple2<>(3.6, 1L),
                new Tuple2<>(5.0, 2L),
                new Tuple2<>(5.4, 3L),
                new Tuple2<>(5.9, 4L),
                new Tuple2<>(6.1, 5L),
                new Tuple2<>(6.3, 6L),
                new Tuple2<>(6.7, 7L),
                new Tuple2<>(9.9, 8L)
        ));
    }

    @Override
    public JavaRDD<BitSet> getValuesBitSets() {
        return sparkContextWrapper.parallelize(Arrays.asList(
                bitSetFromString("000111111"), // 5.4
                bitSetFromString("001111111"), // 5.0
                bitSetFromString("011111111"), // 3.6
                bitSetFromString("000011111"), // 5.9
                bitSetFromString("000011111"), // 5.9
                bitSetFromString("111111111"), // 2.5
                bitSetFromString("000000111"), // 6.3
                bitSetFromString("000000001"), // 9.9
                bitSetFromString("000000011"), // 6.7
                bitSetFromString("000001111")  // 6.1
        ));
    }

    @Override
    public JavaPairRDD<Long, BitSet> getValuesBitSlices() {
        return sparkContextWrapper.parallelizePairs(Arrays.asList(
                new Tuple2<>(0L, bitSetFromString("0000010000")),
                new Tuple2<>(1L, bitSetFromString("0010010000")),
                new Tuple2<>(2L, bitSetFromString("0110010000")),
                new Tuple2<>(3L, bitSetFromString("1110010000")),
                new Tuple2<>(4L, bitSetFromString("1111110000")),
                new Tuple2<>(5L, bitSetFromString("1111110001")),
                new Tuple2<>(6L, bitSetFromString("1111111001")),
                new Tuple2<>(7L, bitSetFromString("1111111011")),
                new Tuple2<>(8L, bitSetFromString("1111111111")),
                new Tuple2<>(-1L, bitSetFromString("000000000"))
        ));
    }
}
