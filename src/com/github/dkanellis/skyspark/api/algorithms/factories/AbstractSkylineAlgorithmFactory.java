package com.github.dkanellis.skyspark.api.algorithms.factories;

import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;

/**
 *
 * @author Dimitris Kanellis
 */
public abstract class AbstractSkylineAlgorithmFactory {

    public abstract SkylineAlgorithm getBlockNestedLoop(SparkContextWrapper sparkContext);
    public abstract SkylineAlgorithm getSortFilterSkyline(SparkContextWrapper sparkContext);
}
