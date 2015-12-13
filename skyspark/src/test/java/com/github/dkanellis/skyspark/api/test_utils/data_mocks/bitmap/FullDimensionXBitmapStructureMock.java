package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.bitmap.BitSlice;
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
    public JavaRDD<Double> getDimensionValues() {
        return BitmapPointsMock.get10Points(sparkContextWrapper).map(Point2D::getX);
    }

    @Override
    public JavaRDD<Double> getDistinctValuesSorted() {
        return sparkContextWrapper.parallelize(Arrays.asList(9.9, 6.7, 6.3, 6.1, 5.9, 5.4, 5.0, 3.6, 2.5));
    }

    @Override
    public JavaPairRDD<Double, Long> getValuesIndexed() {
        return sparkContextWrapper.parallelizePairs(Arrays.asList(
                new Tuple2<>(2.5, 8L),
                new Tuple2<>(3.6, 7L),
                new Tuple2<>(5.0, 6L),
                new Tuple2<>(5.4, 5L),
                new Tuple2<>(5.9, 4L),
                new Tuple2<>(6.1, 3L),
                new Tuple2<>(6.3, 2L),
                new Tuple2<>(6.7, 1L),
                new Tuple2<>(9.9, 0L)
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
    public JavaRDD<BitSlice> getValuesBitSlices() {
        return sparkContextWrapper.parallelize(Arrays.asList(
                new BitSlice(0L, 2.5, bitSetFromString("0000010000")),
                new BitSlice(1L, 3.6, bitSetFromString("0010010000")),
                new BitSlice(2L, 5.0, bitSetFromString("0110010000")),
                new BitSlice(3L, 5.4, bitSetFromString("1110010000")),
                new BitSlice(4L, 5.9, bitSetFromString("1111110000")),
                new BitSlice(5L, 6.1, bitSetFromString("1111110001")),
                new BitSlice(6L, 6.3, bitSetFromString("1111111001")),
                new BitSlice(7L, 6.7, bitSetFromString("1111111011")),
                new BitSlice(8L, 9.9, bitSetFromString("1111111111"))
        ));
    }
}
