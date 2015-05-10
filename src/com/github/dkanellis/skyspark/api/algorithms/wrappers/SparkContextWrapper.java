package com.github.dkanellis.skyspark.api.algorithms.wrappers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class SparkContextWrapper {

    private final JavaSparkContext sparkContext;

    public SparkContextWrapper(String appName, String master) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(appName);
        sparkConf.setMaster("local[4]");

        this.sparkContext = new JavaSparkContext(sparkConf);
    }

    public JavaRDD<String> textFile(String path) {
        return sparkContext.textFile(path);
    }

    public <T> JavaRDD<T> parallelize(List<T> list) {
        return sparkContext.parallelize(list);
    }

    public <K, V> JavaPairRDD<K, V> parallelizePairs(List<Tuple2<K, V>> list) {
        return sparkContext.parallelizePairs(list);
    }

    public void stop() {
        sparkContext.stop();
    }
}
