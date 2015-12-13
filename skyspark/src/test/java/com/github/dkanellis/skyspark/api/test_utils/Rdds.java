package com.github.dkanellis.skyspark.api.test_utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;

public final class Rdds {

    private Rdds() {
        throw new AssertionError("No instances.");
    }

    public static <T> boolean areEqual(JavaRDD<T> expected, JavaRDD<T> actual) {
        List<T> expList = expected.collect();
        List<T> actualList = actual.collect();
        List<T> difference = expected.subtract(actual).collect();

        return difference.isEmpty();
    }

    public static <K, V> boolean areEqual(JavaPairRDD<K, V> expected, JavaPairRDD<K, V> actual) {
        List<Tuple2<K, V>> expList = expected.collect();
        List<Tuple2<K, V>> actualList = actual.collect();
        List<Tuple2<K, V>> difference = expected.subtract(actual).collect();

        return difference.isEmpty();
    }
}
