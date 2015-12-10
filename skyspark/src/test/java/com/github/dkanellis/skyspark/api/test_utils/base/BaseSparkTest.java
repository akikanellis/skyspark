package com.github.dkanellis.skyspark.api.test_utils.base;

import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BaseSparkTest {

    private static SparkContextWrapper sparkContextWrapper;

    @BeforeClass
    public static void setUpClass() {
        sparkContextWrapper = new SparkContextWrapper("Tests", "local[1]");
    }

    @AfterClass
    public static void tearDownClass() {
        sparkContextWrapper.stop();
    }

    public static SparkContextWrapper getSparkContextWrapper() {
        return sparkContextWrapper;
    }
}
