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

    public NewBlockNestedLoop() {
        MedianFinder medianFinder = new MedianFinder();
        FlagAdder flagAdder = new FlagAdder(medianFinder);
        BnlAlgorithm bnlAlgorithm = new BnlAlgorithm();
        LocalSkylineCalculator localSkylineCalculator = new LocalSkylineCalculator(bnlAlgorithm);

        this.divider = new Divider(flagAdder, localSkylineCalculator);
        this.merger = new Merger(bnlAlgorithm);
    }

    public JavaRDD<Point> computeSkylinePoints(JavaRDD<Point> points) {
        if (points.isEmpty()) throw new IllegalArgumentException("Points can't be empty.");

        JavaPairRDD<Flag, Point> localSkylinesWithFlags = divider.divide(points);

        return merger.merge(localSkylinesWithFlags);
    }
}
