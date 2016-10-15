package com.akikanellis.skyspark.api.test_utils.base;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BaseSparkTest {

    private static JavaSparkContext sparkContext;

    @BeforeClass
    public static void setUpClass() {
        sparkContext = new JavaSparkContext("local[*]", "Tests");
    }

    @AfterClass
    public static void tearDownClass() {
        sparkContext.stop();
    }

    protected static JavaSparkContext getSparkContext() {
        return sparkContext;
    }
}
