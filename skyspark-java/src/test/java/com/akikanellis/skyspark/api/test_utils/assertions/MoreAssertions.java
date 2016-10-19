package com.akikanellis.skyspark.api.test_utils.assertions;

import com.akikanellis.skyspark.api.algorithms.Point;
import com.akikanellis.skyspark.api.algorithms.bnl.PointFlag;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.assertj.core.api.Assertions;

import java.awt.geom.Point2D;

public class MoreAssertions extends Assertions {

    public static Point2DRddAssert assertThat(JavaRDD<Point2D> actual) { return new Point2DRddAssert(actual); }

    public static PointRddAssert assertThatP(JavaRDD<Point> actual) { return new PointRddAssert(actual); }

    public static FlagPoint2DRddAssert assertThat(JavaPairRDD<PointFlag, Point2D> actual) {
        return new FlagPoint2DRddAssert(actual);
    }
}
