package com.github.dkanellis.skyspark.api.test_utils.base;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import scala.Tuple2;

import java.util.List;

public abstract class BaseSparkTest {

    private static JavaSparkContext sparkContext;

    @BeforeClass
    public static void setUpClass() {
        sparkContext = new JavaSparkContext("Tests", "local[4]");
    }

    @AfterClass
    public static void tearDownClass() {
        sparkContext.stop();
    }

    protected static JavaSparkContext getSparkContextWrapper() {
        return sparkContext;
    }

    protected <T> JavaRDD<T> toRdd(List<T> listOfElements) {
        return sparkContext.parallelize(listOfElements);
    }

    protected <K, V> JavaPairRDD<K, V> toPairRdd(List<Tuple2<K, V>> pairs) {
        return sparkContext.parallelizePairs(pairs);
    }
}
