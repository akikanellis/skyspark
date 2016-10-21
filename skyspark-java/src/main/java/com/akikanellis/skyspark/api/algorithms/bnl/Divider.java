package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

class Divider implements Serializable {
    private final FlagAdder flagAdder;
    private final LocalSkylineCalculator localSkylineCalculator;

    Divider(FlagAdder flagAdder, LocalSkylineCalculator localSkylineCalculator) {
        this.flagAdder = flagAdder;
        this.localSkylineCalculator = localSkylineCalculator;
    }

    JavaPairRDD<Flag, Point> divide(JavaRDD<Point> points) {
        JavaPairRDD<Flag, Point> flagPoints = flagAdder.addFlags(points);

        return localSkylineCalculator.computeLocalSkylines(flagPoints);
    }
}
