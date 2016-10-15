package com.akikanellis.skyspark.api.test_utils.integration_tests;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.api.helpers.TextFileToPointRDD;
import com.akikanellis.skyspark.api.test_utils.DatasetFiles;
import com.akikanellis.skyspark.api.test_utils.base.BaseSparkTest;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;

import java.awt.geom.Point2D;
import java.util.List;

import static org.junit.Assert.assertTrue;

abstract class BaseSkylineAlgorithmIntegrationTest extends BaseSparkTest {

    private SkylineAlgorithm skylineAlgorithm;
    private TextFileToPointRDD textFileToPointRDD;

    @Before
    public void setUp() {
        skylineAlgorithm = getSkylineAlgorithm();
        textFileToPointRDD = new TextFileToPointRDD(getSparkContext());
    }

    protected abstract SkylineAlgorithm getSkylineAlgorithm();

    protected void smallAnticorrelated() {
        findCorrectSkylines("/ANTICOR_2_10000.txt", "/ANTICOR_2_10000_SKYLINES.txt");
    }

    protected void mediumAnticorrelated() {
        findCorrectSkylines("/ANTICOR_2_100000.txt", "/ANTICOR_2_100000_SKYLINES.txt");
    }

    protected void bigAnticorrelated() {
        findCorrectSkylines("/ANTICOR_2_1000000.txt", "/ANTICOR_2_1000000_SKYLINES.txt");
    }

    protected void smallCorrelated() {
        findCorrectSkylines("/CORREL_2_10000.txt", "/CORREL_2_10000_SKYLINES.txt");
    }

    protected void mediumCorrelated() {
        findCorrectSkylines("/CORREL_2_100000.txt", "/CORREL_2_100000_SKYLINES.txt");
    }

    protected void bigCorrelated() {
        findCorrectSkylines("/CORREL_2_1000000.txt", "/CORREL_2_1000000_SKYLINES.txt");
    }

    protected void smallUniform() {
        findCorrectSkylines("/UNIFORM_2_10000.txt", "/UNIFORM_2_10000_SKYLINES.txt");
    }

    protected void mediumUniform() {
        findCorrectSkylines("/UNIFORM_2_100000.txt", "/UNIFORM_2_100000_SKYLINES.txt");
    }

    protected void bigUniform() {
        findCorrectSkylines("/UNIFORM_2_1000000.txt", "/UNIFORM_2_1000000_SKYLINES.txt");
    }

    private void findCorrectSkylines(String datasetFilePath, String datasetSkylineFilePath) {
        JavaRDD<Point2D> expectedSkylinesRdd = DatasetFiles.getRddFromFile(textFileToPointRDD, datasetSkylineFilePath);

        JavaRDD<Point2D> points = DatasetFiles.getRddFromFile(textFileToPointRDD, datasetFilePath);
        JavaRDD<Point2D> actualSkylinesRdd = skylineAlgorithm.computeSkylinePoints(points);

        List<Point2D> expectedSkylinesList = expectedSkylinesRdd.collect();
        List<Point2D> actualSkylinesList = actualSkylinesRdd.collect();

        assertTrue(expectedSkylinesList.containsAll(actualSkylinesList) && actualSkylinesList.containsAll(expectedSkylinesList));
    }
}
