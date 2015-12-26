package com.github.dkanellis.skyspark.api.test_utils.integration_tests;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.bitmap.Bitmap;
import com.github.dkanellis.skyspark.api.test_utils.categories.algorithms.BitmapTests;
import com.github.dkanellis.skyspark.api.test_utils.categories.combinations.BitmapBigSizeTests;
import com.github.dkanellis.skyspark.api.test_utils.categories.combinations.BitmapMediumSizeTests;
import com.github.dkanellis.skyspark.api.test_utils.categories.combinations.BitmapSmallSizeTests;
import com.github.dkanellis.skyspark.api.test_utils.categories.speeds.SlowTests;
import com.github.dkanellis.skyspark.api.test_utils.categories.types.IntegrationTests;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.List;

@Category({BitmapTests.class, IntegrationTests.class, SlowTests.class})
public class BitmapIntegrationTest extends BaseSkylineAlgorithmIntegrationTest {

    @Override
    protected SkylineAlgorithm getSkylineAlgorithm() {
        return new Bitmap(4);
    }

    @Test
    public void smallFromCollection() {

        List<Point2D> pointsList = Arrays.asList(
                new Point2D.Double(5.4, 4.4),
                new Point2D.Double(5.0, 4.1),
                new Point2D.Double(3.6, 9.0),
                new Point2D.Double(5.9, 4.0),
                new Point2D.Double(5.9, 4.6),
                new Point2D.Double(2.5, 7.3),
                new Point2D.Double(6.3, 3.5),
                new Point2D.Double(9.9, 4.1),
                new Point2D.Double(6.7, 3.3),
                new Point2D.Double(6.1, 3.4)
        );

        JavaRDD<Point2D> points = getSparkContext().parallelize(pointsList);

        getSkylineAlgorithm().computeSkylinePoints(points);
    }

    @Test
    @Category(BitmapSmallSizeTests.class)
    public void smallAnticorrelated() {
        super.smallAnticorrelated();
    }

    @Test
    @Category(BitmapSmallSizeTests.class)
    public void smallCorrelated() {
        super.smallCorrelated();
    }

    @Test
    @Category(BitmapSmallSizeTests.class)
    public void smallUniform() {
        super.smallUniform();
    }

    @Test
    @Category(BitmapMediumSizeTests.class)
    public void mediumAnticorrelated() {
        super.mediumAnticorrelated();
    }

    @Test
    @Category(BitmapMediumSizeTests.class)
    public void mediumCorrelated() {
        super.mediumCorrelated();
    }

    @Test
    @Category(BitmapMediumSizeTests.class)
    public void mediumUniform() {
        super.mediumUniform();
    }

    @Test
    @Category(BitmapBigSizeTests.class)
    public void bigAnticorrelated() {
        super.bigAnticorrelated();
    }

    @Test
    @Category(BitmapBigSizeTests.class)
    public void bigCorrelated() {
        super.bigCorrelated();
    }

    @Test
    @Category(BitmapBigSizeTests.class)
    public void bigUniform() {
        super.bigUniform();
    }
}
