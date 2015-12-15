package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class Bitmap implements SkylineAlgorithm, Serializable {

    private final int numberOfPartitions;

    private final BitmapStructure bitmapOfFirstDimension;
    private final BitmapStructure bitmapOfSecondDimension;

    public Bitmap() {
        this(4);
    }

    public Bitmap(final int numberOfPartitions) {
        checkArgument(numberOfPartitions > 0, "Partitions can't be less than 1.");

        this.numberOfPartitions = numberOfPartitions;
        this.bitmapOfFirstDimension = Injector.getBitmapStructure(numberOfPartitions);
        this.bitmapOfSecondDimension = Injector.getBitmapStructure(numberOfPartitions);
    }

    @Override
    public List<Point2D> getSkylinePoints(JavaRDD<Point2D> points) {
        bitmapOfFirstDimension.init(points.map(Point2D::getX));
        bitmapOfSecondDimension.init(points.map(Point2D::getY));

        JavaRDD<Point2D> skylines = calculateSkylines(points);

        return skylines.collect();
    }

    private JavaRDD<Point2D> calculateSkylines(JavaRDD<Point2D> points) {
        bitmapOfFirstDimension.rdd().collect();
        JavaPairRDD<Double, BitSlice> keyedByDimensionValue = bitmapOfFirstDimension.rdd().keyBy(BitSlice::getDimensionValue);
        keyedByDimensionValue.collectAsMap();
        JavaPairRDD<Double, Point2D> pointsKeyedByDimensionValue = points.keyBy(Point2D::getX);
        JavaPairRDD<Double, Tuple2<Point2D, BitSlice>> joined = pointsKeyedByDimensionValue.join(keyedByDimensionValue);
        joined.take(10).forEach(p -> System.out.println("key"));

        return null;

    }

    private Boolean isSkyline(Point2D point2D) {
        bitmapOfFirstDimension.rdd();
        BitSet bitSliceOfSecondDimension = bitmapOfSecondDimension.getCorrespondingBitSlice(point2D.getY());

        return true;
    }

    private long getPosition(final double pY, JavaPairRDD<Double, Long> distinctPointsOfXDimension) {
        return distinctPointsOfXDimension.count() - distinctPointsOfXDimension.filter(p -> p._1() >= pY).count();
    }

    @Override
    public String toString() {
        return "Bitmap";
    }
}
