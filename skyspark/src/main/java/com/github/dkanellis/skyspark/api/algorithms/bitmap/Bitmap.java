package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

public class Bitmap implements SkylineAlgorithm, Serializable {

    @Override
    public List<Point2D> getSkylinePoints(JavaRDD<Point2D> points) {
        JavaRDD<Double> distinctPointsOfXDimension = getDistinctSorted(points);

        JavaPairRDD<Double, Long> distinctPointsOfYDimension = points
                .map(Point2D::getY)
                .distinct()
                .sortBy(Double::doubleValue, false, 1)
                .zipWithIndex();


        JavaRDD<BitSet> bitsetX = points.map(p -> getBitVectorRepresentation(p.getX(), distinctPointsOfYDimension));

        List<Double> x = distinctPointsOfXDimension.collect();
        List<Tuple2<Double, Long>> y = distinctPointsOfYDimension.collect();

        throw new UnsupportedOperationException("Bitmap is not supported yet.");
    }

    /**
     * We use ascending order because our points dominate each other when they are less in every dimension.
     *
     * @param points
     * @return
     */
    JavaRDD<Double> getDistinctSorted(JavaRDD<Point2D> points) {
        return points
                .map(Point2D::getX)
                .distinct()
                .sortBy(Double::doubleValue, true, 1);
    }

    private BitSet getBitVectorRepresentation(final double p, JavaPairRDD<Double, Long> distinctPoints) {
        BitSet bitSet = new BitSet();
        bitSet.set(0, (int) getPosition(p, distinctPoints));
        return bitSet;
    }

    private long getPosition(final double pY, JavaPairRDD<Double, Long> distinctPointsOfXDimension) {
        return distinctPointsOfXDimension.count() - distinctPointsOfXDimension.filter(p -> p._1() >= pY).count();
    }

    @Override
    public String toString() {
        return "Bitmap";
    }
}
