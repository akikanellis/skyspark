package com.github.dkanellis.skyspark.api.test_utils.integration_tests;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.helpers.TextFileToPointRDD;
import com.github.dkanellis.skyspark.api.test_utils.DatasetFiles;
import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;

import java.awt.geom.Point2D;
import java.util.List;

import static org.junit.Assert.assertTrue;

public abstract class BaseSkylineAlgorithmIntegrationTest extends BaseSparkTest {

    private SkylineAlgorithm skylineAlgorithm;
    private TextFileToPointRDD textFileToPointRDD;

    @Before
    public void setUp() {
        skylineAlgorithm = getSkylineAlgorithm();
        textFileToPointRDD = new TextFileToPointRDD(getSparkContextWrapper());
    }

    protected abstract SkylineAlgorithm getSkylineAlgorithm();

    public void smallAnticorrelated() {
        findCorrectSkylines("/ANTICOR_2_10000.txt", "/ANTICOR_2_10000_SKYLINES.txt");
    }

    public void mediumAnticorrelated() {
        findCorrectSkylines("/ANTICOR_2_100000.txt", "/ANTICOR_2_100000_SKYLINES.txt");
    }

    public void bigAnticorrelated() {
        findCorrectSkylines("/ANTICOR_2_1000000.txt", "/ANTICOR_2_1000000_SKYLINES.txt");
    }

    public void smallCorrelated() {
        findCorrectSkylines("/CORREL_2_10000.txt", "/CORREL_2_10000_SKYLINES.txt");
    }

    public void mediumCorrelated() {
        findCorrectSkylines("/CORREL_2_100000.txt", "/CORREL_2_100000_SKYLINES.txt");
    }

    public void bigCorrelated() {
        findCorrectSkylines("/CORREL_2_1000000.txt", "/CORREL_2_1000000_SKYLINES.txt");
    }

    public void smallUniform() {
        findCorrectSkylines("/UNIFORM_2_10000.txt", "/UNIFORM_2_10000_SKYLINES.txt");
    }

    public void mediumUniform() {
        findCorrectSkylines("/UNIFORM_2_100000.txt", "/UNIFORM_2_100000_SKYLINES.txt");
    }

    public void bigUniform() {
        findCorrectSkylines("/UNIFORM_2_1000000.txt", "/UNIFORM_2_1000000_SKYLINES.txt");
    }

    private void findCorrectSkylines(String datasetFilePath, String datasetSkylineFilePath) {
        JavaRDD<Point2D> expectedSkylinesRdd = DatasetFiles.getRddFromFile(textFileToPointRDD, datasetSkylineFilePath);

        JavaRDD<Point2D> points = DatasetFiles.getRddFromFile(textFileToPointRDD, datasetFilePath);
        List<Point2D> actualSkylines = skylineAlgorithm.getSkylinePoints(points);
        actualSkylines.removeAll(expectedSkylinesRdd.collect());

        assertTrue(actualSkylines.isEmpty());
    }
}
