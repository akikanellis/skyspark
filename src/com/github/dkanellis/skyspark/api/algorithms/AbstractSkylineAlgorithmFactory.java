package com.github.dkanellis.skyspark.api.algorithms;

import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;

/**
 *
 * @author Dimitris Kanellis
 */
public abstract class AbstractSkylineAlgorithmFactory {

    public abstract SkylineAlgorithm getBlockNestedLoop(SparkContextWrapper sparkContext);
}
