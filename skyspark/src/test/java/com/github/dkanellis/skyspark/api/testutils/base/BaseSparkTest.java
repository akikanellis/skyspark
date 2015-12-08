package com.github.dkanellis.skyspark.api.testUtils.base;

import com.github.dkanellis.skyspark.api.helpers.SparkContextWrapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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

    public static SparkContextWrapper getSparkContextWrapper() {
        return sparkContextWrapper;
    }
}
