package com.akikanellis.skyspark.api.test_utils.integration_tests;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.api.algorithms.sfs.SortFilterSkyline;
import com.akikanellis.skyspark.api.test_utils.categories.algorithms.SortFilterSkylineTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.SortFilterSkylineBigSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.SortFilterSkylineMediumSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.SortFilterSkylineSmallSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.speeds.SlowTests;
import com.akikanellis.skyspark.api.test_utils.categories.types.IntegrationTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
