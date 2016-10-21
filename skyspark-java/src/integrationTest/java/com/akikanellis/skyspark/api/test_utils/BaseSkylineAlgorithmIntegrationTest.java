package com.akikanellis.skyspark.api.test_utils;

import com.akikanellis.skyspark.api.algorithms.OldSkylineAlgorithm;
import com.akikanellis.skyspark.api.helpers.TextFileToPointRDD;
import com.akikanellis.skyspark.data.DatasetFiles;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Rule;

import java.awt.geom.Point2D;

import static com.akikanellis.skyspark.api.test_utils.assertions.MoreAssertions.assertThat;

public abstract class BaseSkylineAlgorithmIntegrationTest {
    @Rule public SparkContextRule sc = new SparkContextRule();

    private OldSkylineAlgorithm skylineAlgorithm;
    private TextFileToPointRDD textFileToPointRDD;

    @Before
    public void setUp() {
        skylineAlgorithm = getSkylineAlgorithm();
        textFileToPointRDD = new TextFileToPointRDD(sc.get());
    }

    protected abstract OldSkylineAlgorithm getSkylineAlgorithm();

    protected void smallAnticorrelated() {
        findCorrectSkylines(DatasetFiles.ANTICOR_2_10000);
    }

    protected void mediumAnticorrelated() {
        findCorrectSkylines(DatasetFiles.ANTICOR_2_100000);
    }

    protected void bigAnticorrelated() {
        findCorrectSkylines(DatasetFiles.ANTICOR_2_1000000);
    }

    protected void smallCorrelated() {
        findCorrectSkylines(DatasetFiles.CORREL_2_10000);
    }

    protected void mediumCorrelated() {
        findCorrectSkylines(DatasetFiles.CORREL_2_100000);
    }

    protected void bigCorrelated() {
        findCorrectSkylines(DatasetFiles.CORREL_2_1000000);
    }

    protected void smallUniform() {
        findCorrectSkylines(DatasetFiles.UNIFORM_2_10000);
    }

    protected void mediumUniform() {
        findCorrectSkylines(DatasetFiles.UNIFORM_2_100000);
    }

    protected void bigUniform() {
        findCorrectSkylines(DatasetFiles.UNIFORM_2_1000000);
    }

    private void findCorrectSkylines(DatasetFiles dataset) {
        JavaRDD<Point2D> expectedSkylines = textFileToPointRDD.getPointRddFromTextFile(dataset.skylinesPath());
        JavaRDD<Point2D> points = textFileToPointRDD.getPointRddFromTextFile(dataset.pointsPath());

        JavaRDD<Point2D> actualSkylines = skylineAlgorithm.computeSkylinePoints(points);

        assertThat(actualSkylines).containsOnlyElementsOf(expectedSkylines);
    }
}
