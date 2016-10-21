package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.Serializable;

class LocalSkylineCalculator implements Serializable {
    private final BnlAlgorithm bnlAlgorithm;

    LocalSkylineCalculator(BnlAlgorithm bnlAlgorithm) { this.bnlAlgorithm = bnlAlgorithm; }

    JavaPairRDD<Flag, Point> computeLocalSkylines(JavaPairRDD<Flag, Point> flagPoints) {
        return flagPoints
                .groupByKey()
                .flatMapValues(bnlAlgorithm::computeSkylinesWithoutPreComparison);
    }
}
