package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.bitmap.BitSlice;
import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.BitSet;

import static com.github.dkanellis.skyspark.api.utils.BitSets.bitSetFromString;

public class FullDimensionYBitmapStructureMock implements FullBitmapStructureMock {

    private final SparkContextWrapper sparkContextWrapper;

    public FullDimensionYBitmapStructureMock(SparkContextWrapper sparkContextWrapper) {
        this.sparkContextWrapper = sparkContextWrapper;
    }

    @Override
    public Long getSizeOfUniqueValues() {
        return null;
    }

    @Override
    public JavaRDD<Double> getDimensionValues() {
        return BitmapPointsMock.get10Points(sparkContextWrapper).map(Point2D::getY);
    }

    @Override
    public JavaPairRDD<Double, Long> getValuesIndexed() {
        return null;//sparkContextWrapper.parallelizePairs(Arrays.asList(
//                new Tuple2<>(0L, 3.3),
//                new Tuple2<>(1L, 3.4),
//                new Tuple2<>(2L, 3.5),
//                new Tuple2<>(3L, 4.0),
//                new Tuple2<>(4L, 4.1),
//                new Tuple2<>(5L, 4.4),
//                new Tuple2<>(6L, 4.6),
//                new Tuple2<>(7L, 7.3),
//                new Tuple2<>(8L, 9.0)
//        ));
    }

    @Override
    public JavaRDD<BitSet> getValuesBitSets() {
        return sparkContextWrapper.parallelize(Arrays.asList(
                bitSetFromString("000001111"), // 4.4
                bitSetFromString("000011111"), // 4.1
                bitSetFromString("000000001"), // 9.0
                bitSetFromString("000111111"), // 4.0
                bitSetFromString("000000111"), // 4.6
                bitSetFromString("000000011"), // 7.3
                bitSetFromString("001111111"), // 3.5
                bitSetFromString("000011111"), // 4.1
                bitSetFromString("111111111"), // 3.3
                bitSetFromString("011111111")  // 3.4
        ));
    }

    @Override
    public JavaPairRDD<Long, BitSlice> getValuesBitSlices() {
        return null;
    }
}
