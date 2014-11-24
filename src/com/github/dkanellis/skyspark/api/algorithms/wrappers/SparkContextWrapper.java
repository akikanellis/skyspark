package com.github.dkanellis.skyspark.api.algorithms.wrappers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

/**
 *
 * @author Dimitris Kanellis
 */
public class SparkContextWrapper {

    private final SparkConf sparConf;
    private final JavaSparkContext sparkContext;

    public SparkContextWrapper(String appName, String master) {
        this.sparConf = new SparkConf();
        this.sparConf.setAppName(appName);
        this.sparConf.setMaster(master);

        this.sparkContext = new JavaSparkContext(sparConf);
    }

    public JavaRDD<String> textFile(String path) {
        return sparkContext.textFile(path);
    }

}
