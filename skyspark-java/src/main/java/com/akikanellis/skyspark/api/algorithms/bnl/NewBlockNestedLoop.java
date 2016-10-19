package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public class NewBlockNestedLoop implements Serializable {
    private final Divider divider;
    private final Merger merger;

    NewBlockNestedLoop(Divider divider, Merger merger) {
        this.divider = divider;
        this.merger = merger;
    }

    public JavaRDD<Point> computeSkylinePoints(JavaRDD<Point> points) {
        if (points.isEmpty()) throw new IllegalArgumentException("Points can't be empty.");

        JavaPairRDD<Flag, Point> localSkylinesWithFlags = divider.divide(points);
        JavaRDD<Point> skylines = merger.merge(localSkylinesWithFlags);

        return skylines;
    }
}
