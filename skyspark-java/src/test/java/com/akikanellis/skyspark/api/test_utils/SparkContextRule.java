package com.akikanellis.skyspark.api.test_utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import scala.Tuple2;

import java.util.List;

/**
 * A Rule for starting a Spark context before the test and stopping it after the test. It also provides access to some
 * common Spark context functionality.
 */
public class SparkContextRule implements TestRule {
    private JavaSparkContext sparkContext;

    public JavaSparkContext get() { return sparkContext; }

    public <T> JavaRDD<T> parallelize(List<T> list) { return sparkContext.parallelize(list); }

    public <K, V> JavaPairRDD<K, V> parallelizePairs(List<Tuple2<K, V>> list) {
        return sparkContext.parallelizePairs(list);
    }

    @Override public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override public void evaluate() throws Throwable {
                sparkContext = new JavaSparkContext("local[*]", "Tests");

                base.evaluate();

                sparkContext.stop();
                sparkContext = null;
            }
        };
    }
}
