package com.github.dkanellis.skyspark.api.test_utils.data_mocks.bitmap;

import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.BitSet;

import static com.github.dkanellis.skyspark.api.utils.BitSets.bitSetfromString;

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
        return sparkContextWrapper.parallelize(Arrays.asList(2.5, 3.6, 5.0, 5.4, 5.9, 6.1, 6.3, 6.7, 9.9));
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
