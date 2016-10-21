package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

class Merger implements Serializable {
    private final BnlAlgorithm bnlAlgorithm;

    Merger(BnlAlgorithm bnlAlgorithm) { this.bnlAlgorithm = bnlAlgorithm; }

    JavaRDD<Point> merge(JavaPairRDD<Flag, Point> flagPoints) {
        JavaRDD<List<Tuple2<Flag, Point>>> inSinglePartition = flagPoints
                .coalesce(1)
                .glom();

        return inSinglePartition.flatMap(bnlAlgorithm::computeSkylinesWithPreComparison);
    }
}
