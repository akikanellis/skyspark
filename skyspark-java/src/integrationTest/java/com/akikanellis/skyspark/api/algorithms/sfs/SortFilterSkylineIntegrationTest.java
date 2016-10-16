package com.akikanellis.skyspark.api.algorithms.sfs;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.api.test_utils.BaseSkylineAlgorithmIntegrationTest;
import org.junit.Test;

public class SortFilterSkylineIntegrationTest extends BaseSkylineAlgorithmIntegrationTest {

    @Override
    protected SkylineAlgorithm getSkylineAlgorithm() {
        return new SortFilterSkyline();
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

    @Test
    public void mediumAnticorrelated() {
        super.mediumAnticorrelated();
    }

    @Test
    public void mediumCorrelated() {
        super.mediumCorrelated();
    }

    @Test
    public void mediumUniform() {
        super.mediumUniform();
    }

    @Test
    public void bigAnticorrelated() {
        super.bigAnticorrelated();
    }

    @Test
    public void bigCorrelated() {
        super.bigCorrelated();
    }

    @Test
    public void bigUniform() {
        super.bigUniform();
    }
}
