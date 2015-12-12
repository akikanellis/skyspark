package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.awt.geom.Point2D;
import java.util.BitSet;

class BitmapStructure {

    private final JavaRDD<Point2D> points;
    private final int numberOfPartitions;

    private JavaRDD<BitSet> dimensionXBitmap;
    private JavaRDD<BitSet> dimensionYBitmap;

    BitmapStructure(@NotNull JavaRDD<Point2D> points, final int numberOfPartitions) {
        this.points = points;
        this.numberOfPartitions = numberOfPartitions;
    }

    public void /* TODO change */ create() {
        JavaRDD<Double> distinctPointsOfXDimension = getDistinctSorted(points, 1);
        JavaRDD<Double> distinctPointsOfYDimension = getDistinctSorted(points, 2);

        JavaPairRDD<Long, Double> uniqueXValuesIndexed = mapWithIndex(distinctPointsOfXDimension);
    }

    JavaPairRDD<Long, Double> mapWithIndex(JavaRDD<Double> distinctPointsOfXDimension) {
        return distinctPointsOfXDimension.zipWithIndex().mapToPair(Tuple2::swap);
    }

    // We use ascending order because our points dominate each other when they are less in every dimension.
    JavaRDD<Double> getDistinctSorted(JavaRDD<Point2D> points, final int dimension) {
        return points
                .map(dimension == 1 ? Point2D::getX : Point2D::getY)
                .distinct()
                .sortBy(Double::doubleValue, true, numberOfPartitions);
    }
}
