package com.github.dkanellis.skyspark.api.algorithms;

import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;

/**
 *
 * @author Dimitris Kanellis
 */
public class SkylineAlgorithmFactory extends AbstractSkylineAlgorithmFactory {

    @Override
    public SkylineAlgorithm getBlockNestedLoop(SparkContextWrapper sparkContext) {
        return new BlockNestedLoop(sparkContext);
    }
}
