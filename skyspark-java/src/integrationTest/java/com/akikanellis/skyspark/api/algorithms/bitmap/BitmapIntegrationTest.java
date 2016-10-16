package com.akikanellis.skyspark.api.algorithms.bitmap;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.api.test_utils.BaseSkylineAlgorithmIntegrationTest;
import com.akikanellis.skyspark.api.test_utils.categories.algorithms.BitmapTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.BitmapBigSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.BitmapMediumSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.BitmapSmallSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.speeds.SlowTests;
import com.akikanellis.skyspark.api.test_utils.categories.types.IntegrationTests;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

    @Ignore("Bitmap is currently misbehaving in medium sized data.")
    @Test
    @Category(BitmapMediumSizeTests.class)
    public void mediumAnticorrelated() {
        super.mediumAnticorrelated();
    }

    @Ignore("Bitmap is currently misbehaving in medium sized data.")
    @Test
    @Category(BitmapMediumSizeTests.class)
    public void mediumCorrelated() {
        super.mediumCorrelated();
    }

    @Ignore("Bitmap is currently misbehaving in medium sized data.")
    @Test
    @Category(BitmapMediumSizeTests.class)
    public void mediumUniform() {
        super.mediumUniform();
    }

    @Ignore("Bitmap is currently misbehaving in big sized data.")
    @Test
    @Category(BitmapBigSizeTests.class)
    public void bigAnticorrelated() {
        super.bigAnticorrelated();
    }

    @Ignore("Bitmap is currently misbehaving in big sized data.")
    @Test
    @Category(BitmapBigSizeTests.class)
    public void bigCorrelated() {
        super.bigCorrelated();
    }

    @Ignore("Bitmap is currently misbehaving in big sized data.")
    @Test
    @Category(BitmapBigSizeTests.class)
    public void bigUniform() {
        super.bigUniform();
    }
}
