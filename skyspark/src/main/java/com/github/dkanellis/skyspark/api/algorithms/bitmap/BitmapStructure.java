package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.util.BitSet;

class BitmapStructure {

    private final JavaRDD<Double> dimensionValues;
    private final int numberOfPartitions;

    private JavaRDD<BitSet> dimensionBitmap;

    BitmapStructure(@NotNull JavaRDD<Double> dimensionValues, final int numberOfPartitions) {
        this.dimensionValues = dimensionValues;
        this.numberOfPartitions = numberOfPartitions;
    }

    public void /* TODO change */ create() {
        JavaRDD<Double> distinctSortedPoints = getDistinctSorted();

        JavaPairRDD<Long, Double> indexed = mapWithIndex(distinctSortedPoints);

        //avaRDD<BitSet> bitSets = points.map(p -> );
    }

    JavaPairRDD<Long, Double> mapWithIndex(JavaRDD<Double> distinctPointsOfDimension) {
        return distinctPointsOfDimension.zipWithIndex().mapToPair(Tuple2::swap);
    }

    // We use ascending order because our points dominate each other when they are less in every dimension.
    JavaRDD<Double> getDistinctSorted() {
        return dimensionValues
                .distinct()
                .sortBy(Double::doubleValue, true, numberOfPartitions);
    }
}
