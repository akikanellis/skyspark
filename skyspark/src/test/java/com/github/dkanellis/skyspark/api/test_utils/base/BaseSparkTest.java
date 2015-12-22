package com.github.dkanellis.skyspark.api.test_utils.base;

import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import scala.Tuple2;

import java.util.List;

public abstract class BaseSparkTest {

    private static SparkContextWrapper sparkContextWrapper;

    @BeforeClass
    public static void setUpClass() {
        sparkContextWrapper = new SparkContextWrapper("Tests", "local[4]");
    }

    @AfterClass
    public static void tearDownClass() {
        sparkContextWrapper.stop();
    }

    protected static SparkContextWrapper getSparkContextWrapper() {
        return sparkContextWrapper;
    }

    protected <T> JavaRDD<T> toRdd(List<T> listOfElements) {
        return sparkContextWrapper.parallelize(listOfElements);
    }

    protected <K, V> JavaPairRDD<K, V> toPairRdd(List<Tuple2<K, V>> pairs) {
        return sparkContextWrapper.parallelizePairs(pairs);
    }
}
