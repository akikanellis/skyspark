package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Map;

/**
 * Data structure which holds the bitmap in the form of a broadcast to all the nodes.
 */
public class BitmapStructure implements Serializable {

    /**
     * The broadcasted bitmap.
     */
    private final Broadcast<Map<Tuple2<Integer, Integer>, BitSet>> broadcastedBitmap;

    public BitmapStructure(JavaPairRDD<Tuple2<Integer, Integer>, BitSet> bitmap) {
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(bitmap.context());
        broadcastedBitmap = sparkContext.broadcast(bitmap.collectAsMap());
    }

    /**
     * Given a <Dimension, Ranking> key, return the corresponding bit slice.
     *
     * @param dimension the dimension we want.
     * @param rank      the ranking we want.
     * @return the corresponding bit slice.
     */
    public BitSet getBitSlice(final int dimension, final int rank) {
        if (rank < 0) {
            return new BitSet();
        }

        Tuple2<Integer, Integer> key = new Tuple2<>(dimension, rank);

        return (BitSet) broadcastedBitmap.value().get(key).clone();
    }
}
