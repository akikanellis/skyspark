package com.akikanellis.skyspark.api.test_utils.assertions;

import com.akikanellis.skyspark.api.algorithms.bnl.PointFlag;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.assertj.core.api.Assertions;

import java.awt.geom.Point2D;

public class MoreAssertions extends Assertions {

    public static PointRddAssert assertThat(JavaRDD<Point2D> actual) { return new PointRddAssert(actual); }

    public static FlagPointRddAssert assertThat(JavaPairRDD<PointFlag, Point2D> actual) {
        return new FlagPointRddAssert(actual);
    }
}
