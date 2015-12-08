package com.github.dkanellis.skyspark.api.testUtils.integrationTests;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.bitmap.Bitmap;
import com.github.dkanellis.skyspark.api.testUtils.categories.algorithms.BitmapTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.combinations.BitmapBigSizeTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.combinations.BitmapMediumSizeTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.combinations.BitmapSmallSizeTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.speeds.SlowTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.types.IntegrationTests;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Ignore
@Category({BitmapTests.class, IntegrationTests.class, SlowTests.class})
public class BitmapIntegrationTest extends BaseSkylineAlgorithmIntegrationTest {

    @Override
    protected SkylineAlgorithm getSkylineAlgorithm() {
        return new Bitmap();
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
