package com.akikanellis.skyspark.api.algorithms.bitmap;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.api.test_utils.BaseSkylineAlgorithmIntegrationTest;
import org.junit.Ignore;
import org.junit.Test;

public class BitmapIntegrationTest extends BaseSkylineAlgorithmIntegrationTest {

    @Override
    protected SkylineAlgorithm getSkylineAlgorithm() {
        return new Bitmap();
    }

    @Test
    public void smallAnticorrelated() {
        super.smallAnticorrelated();
    }

    @Test
    public void smallCorrelated() {
        super.smallCorrelated();
    }

    @Test
    public void smallUniform() {
        super.smallUniform();
    }

    @Ignore("Bitmap is currently misbehaving in medium sized data.")
    @Test
    public void mediumAnticorrelated() {
        super.mediumAnticorrelated();
    }

    @Ignore("Bitmap is currently misbehaving in medium sized data.")
    @Test
    public void mediumCorrelated() {
        super.mediumCorrelated();
    }

    @Ignore("Bitmap is currently misbehaving in medium sized data.")
    @Test
    public void mediumUniform() {
        super.mediumUniform();
    }

    @Ignore("Bitmap is currently misbehaving in big sized data.")
    @Test
    public void bigAnticorrelated() {
        super.bigAnticorrelated();
    }

    @Ignore("Bitmap is currently misbehaving in big sized data.")
    @Test
    public void bigCorrelated() {
        super.bigCorrelated();
    }

    @Ignore("Bitmap is currently misbehaving in big sized data.")
    @Test
    public void bigUniform() {
        super.bigUniform();
    }
}
