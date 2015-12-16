package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class Bitmap implements SkylineAlgorithm, Serializable {

    private final int numberOfPartitions;

    private final BitmapStructure bitmapOfFirstDimension;
    private final BitmapStructure bitmapOfSecondDimension;
    private final SparkContextWrapper sparkContextWrapper;

    public Bitmap(SparkContextWrapper sparkContextWrapper) {
        this(sparkContextWrapper, 4);
    }

    public Bitmap(SparkContextWrapper sparkContextWrapper, final int numberOfPartitions) {
        checkArgument(numberOfPartitions > 0, "Partitions can't be less than 1.");

        this.sparkContextWrapper = sparkContextWrapper;
        this.numberOfPartitions = numberOfPartitions;
        this.bitmapOfFirstDimension = Injector.getBitmapStructure(sparkContextWrapper, numberOfPartitions);
        this.bitmapOfSecondDimension = Injector.getBitmapStructure(sparkContextWrapper, numberOfPartitions);
    }

    @Override
    public List<Point2D> getSkylinePoints(JavaRDD<Point2D> points) {
        bitmapOfFirstDimension.init(points.map(Point2D::getX));
        bitmapOfSecondDimension.init(points.map(Point2D::getY));

        JavaRDD<Point2D> skylines = calculateSkylines(points);

        return skylines.collect();
    }

    private JavaRDD<Point2D> calculateSkylines(JavaRDD<Point2D> points) {
        JavaPairRDD<Point2D, Long> pointsWithRankingsOfFirstDimension
                = points.keyBy(Point2D::getX)
                .join(bitmapOfFirstDimension.rankingsRdd())
                .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2()));

        JavaPairRDD<Point2D, Long> pointsWithRankingsOfSecondDimension
                = points.keyBy(Point2D::getY)
                .join(bitmapOfSecondDimension.rankingsRdd())
                .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2()));

        JavaPairRDD<Point2D, Tuple2<Long, Long>> joined = pointsWithRankingsOfFirstDimension.join(pointsWithRankingsOfSecondDimension);

        JavaPairRDD<Point2D, Tuple2<BitSlice, BitSlice>> withCurrentBitSlices
                = joined.keyBy(p -> p._2()._1())
                .join(bitmapOfFirstDimension.bitSlicesRdd())
                .keyBy(p -> p._2()._1()._2()._2())
                .join(bitmapOfSecondDimension.bitSlicesRdd())
                .mapToPair(p -> new Tuple2<>(p._2()._1()._2()._1()._1(), new Tuple2<>(p._2()._1()._2()._2(), p._2()._2())));

        JavaPairRDD<Point2D, Tuple2<Tuple2<BitSlice, BitSlice>, Tuple2<BitSlice, BitSlice>>> withCurrentAndPreviousBitSlices =
                getWithCurrentAndPreviousBitSlices(points, withCurrentBitSlices);

//        .take(100)
//                .forEach(p -> System.out.printf("(point=%s, bitslice1=%s, bitslice2=%s)\n",
//                p._1(), p._2()._1(), p._2()._2()));

        //JavaPairRDD<Double, Tuple2<Point2D, BitSlice>> joined = pointsKeyedByDimensionValue.join(keyedByDimensionValue);
        //joined.take(10).forEach(p -> System.out.println("key"));

        return null;

    }

    private JavaPairRDD<Point2D, Tuple2<Tuple2<BitSlice, BitSlice>, Tuple2<BitSlice, BitSlice>>> getWithCurrentAndPreviousBitSlices(
            JavaRDD<Point2D> points, JavaPairRDD<Point2D, Tuple2<BitSlice, BitSlice>> withCurrentBitSlices) {
        JavaPairRDD<Point2D, Long> pointsWithRankingsOfFirstDimension
                = points.keyBy(Point2D::getX)
                .join(bitmapOfFirstDimension.rankingsRdd())
                .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2() - 1));

        JavaPairRDD<Point2D, Long> pointsWithRankingsOfSecondDimension
                = points.keyBy(Point2D::getY)
                .join(bitmapOfSecondDimension.rankingsRdd())
                .mapToPair(p -> new Tuple2<>(p._2()._1(), p._2()._2() - 1));

        JavaPairRDD<Point2D, Tuple2<Long, Long>> joined = pointsWithRankingsOfFirstDimension.join(pointsWithRankingsOfSecondDimension);

        JavaPairRDD<Point2D, Tuple2<BitSlice, BitSlice>> withPreviousBitSlices
                = joined.keyBy(p -> p._2()._1())
                .join(bitmapOfFirstDimension.bitSlicesRdd())
                .keyBy(p -> p._2()._1()._2()._2())
                .join(bitmapOfSecondDimension.bitSlicesRdd())
                .mapToPair(p -> new Tuple2<>(p._2()._1()._2()._1()._1(), new Tuple2<>(p._2()._1()._2()._2(), p._2()._2())));

        withPreviousBitSlices.filter(p -> p._2()._1().getIndex() == 0).take(100)
                .forEach(p -> System.out.printf("(point=%s, bitslice1=%s, bitslice2=%s)\n",
                        p._1(), p._2()._1(), p._2()._2()));

        return null;
    }

    private Boolean isSkyline(Point2D point2D) {
        bitmapOfFirstDimension.bitSlicesRdd();
        //BitSet bitSliceOfSecondDimension = bitmapOfSecondDimension.getCorrespondingBitSlice(point2D.getY());

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
