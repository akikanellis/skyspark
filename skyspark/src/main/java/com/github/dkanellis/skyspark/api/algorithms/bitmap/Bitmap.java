package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class Bitmap implements SkylineAlgorithm, Serializable {

    private final int numberOfPartitions;

    public Bitmap() {
        this(4);
    }

    public Bitmap(final int numberOfPartitions) {
        checkArgument(numberOfPartitions > 0, "Partitions can't be less than 1.");

        this.numberOfPartitions = numberOfPartitions;
    }

    @Override
    public List<Point2D> getSkylinePoints(JavaRDD<Point2D> points) {
        BitmapStructure bitmapStructure = new BitmapStructure(points, numberOfPartitions);

        //JavaRDD<BitSet> bitsetX = points.map(p -> getBitVectorRepresentation(p.getX(), distinctPointsOfYDimension));

        //List<Double> x = distinctPointsOfXDimension.collect();
        //List<Tuple2<Double, Long>> y = distinctPointsOfYDimension.collect();

        throw new UnsupportedOperationException("Bitmap is not supported yet.");
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
