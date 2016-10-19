package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

class Divider implements Serializable {
    JavaPairRDD<Flag, Point> divide(JavaRDD<Point> points) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
