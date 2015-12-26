package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Map;

public class BitmapStructure implements Serializable {

    private final Broadcast<Map<Tuple2<Integer, Integer>, BitSet>> broadcastedBitmap;

    public BitmapStructure(JavaPairRDD<Tuple2<Integer, Integer>, BitSet> bitmap) {
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(bitmap.context());
        broadcastedBitmap = sparkContext.broadcast(bitmap.collectAsMap());
    }

    BitSet getBitSlice(final int dimension, final int rank) {
        if (rank < 0) {
            return new BitSet();
        }

        Tuple2<Integer, Integer> key = new Tuple2<>(dimension, rank);

        return (BitSet) broadcastedBitmap.value().get(key).clone();
    }
}
