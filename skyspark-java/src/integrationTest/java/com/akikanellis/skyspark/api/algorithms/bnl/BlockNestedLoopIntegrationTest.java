package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.api.test_utils.BaseSkylineAlgorithmIntegrationTest;
import com.akikanellis.skyspark.api.test_utils.categories.algorithms.BlockNestedLoopTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.BlockNestedLoopBigSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.BlockNestedLoopMediumSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.combinations.BlockNestedLoopSmallSizeTests;
import com.akikanellis.skyspark.api.test_utils.categories.speeds.SlowTests;
import com.akikanellis.skyspark.api.test_utils.categories.types.IntegrationTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({BlockNestedLoopTests.class, IntegrationTests.class, SlowTests.class})
public class BlockNestedLoopIntegrationTest extends BaseSkylineAlgorithmIntegrationTest {

    @Override
    protected SkylineAlgorithm getSkylineAlgorithm() {
        return new BlockNestedLoop();
    }

    @Test
    @Category(BlockNestedLoopSmallSizeTests.class)
    public void smallAnticorrelated() {
        super.smallAnticorrelated();
    }

    @Test
    @Category(BlockNestedLoopSmallSizeTests.class)
    public void smallCorrelated() {
        super.smallCorrelated();
    }

    @Test
    @Category(BlockNestedLoopSmallSizeTests.class)
    public void smallUniform() {
        super.smallUniform();
    }

    @Test
    @Category(BlockNestedLoopMediumSizeTests.class)
    public void mediumAnticorrelated() {
        super.mediumAnticorrelated();
    }

    @Test
    @Category(BlockNestedLoopMediumSizeTests.class)
    public void mediumCorrelated() {
        super.mediumCorrelated();
    }

    @Test
    @Category(BlockNestedLoopMediumSizeTests.class)
    public void mediumUniform() {
        super.mediumUniform();
    }

    @Test
    @Category(BlockNestedLoopBigSizeTests.class)
    public void bigAnticorrelated() {
        super.bigAnticorrelated();
    }

    @Test
    @Category(BlockNestedLoopBigSizeTests.class)
    public void bigCorrelated() {
        super.bigCorrelated();
    }

    @Test
    @Category(BlockNestedLoopBigSizeTests.class)
    public void bigUniform() {
        super.bigUniform();
    }
}
