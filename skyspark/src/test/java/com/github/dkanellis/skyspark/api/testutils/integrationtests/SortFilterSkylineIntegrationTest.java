package com.github.dkanellis.skyspark.api.testUtils.integrationTests;

import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.sfs.SortFilterSkyline;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.github.dkanellis.skyspark.api.testUtils.categories.algorithms.SortFilterSkylineTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.combinations.SortFilterSkylineBigSizeTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.combinations.SortFilterSkylineMediumSizeTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.combinations.SortFilterSkylineSmallSizeTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.speeds.SlowTests;
import com.github.dkanellis.skyspark.api.testUtils.categories.types.IntegrationTests;

@Category({SortFilterSkylineTests.class, IntegrationTests.class, SlowTests.class})
public class SortFilterSkylineIntegrationTest extends BaseSkylineAlgorithmIntegrationTest {

    @Override
    protected SkylineAlgorithm getSkylineAlgorithm() {
        return new SortFilterSkyline();
    }

    @Test
    @Category(SortFilterSkylineSmallSizeTests.class)
    public void smallAnticorrelated() {
        super.smallAnticorrelated();
    }

    @Test
    @Category(SortFilterSkylineSmallSizeTests.class)
    public void smallCorrelated() {
        super.smallCorrelated();
    }

    @Test
    @Category(SortFilterSkylineSmallSizeTests.class)
    public void smallUniform() {
        super.smallUniform();
    }

    @Test
    @Category(SortFilterSkylineMediumSizeTests.class)
    public void mediumAnticorrelated() {
        super.mediumAnticorrelated();
    }

    @Test
    @Category(SortFilterSkylineMediumSizeTests.class)
    public void mediumCorrelated() {
        super.mediumCorrelated();
    }

    @Test
    @Category(SortFilterSkylineMediumSizeTests.class)
    public void mediumUniform() {
        super.mediumUniform();
    }

    @Test
    @Category(SortFilterSkylineBigSizeTests.class)
    public void bigAnticorrelated() {
        super.bigAnticorrelated();
    }

    @Test
    @Category(SortFilterSkylineBigSizeTests.class)
    public void bigCorrelated() {
        super.bigCorrelated();
    }

    @Test
    @Category(SortFilterSkylineBigSizeTests.class)
    public void bigUniform() {
        super.bigUniform();
    }
}
