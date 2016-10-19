package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

class Merger implements Serializable {

    JavaRDD<Point> merge(JavaPairRDD<Flag, Point> flagPoints) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
