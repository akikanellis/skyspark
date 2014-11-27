package com.github.dkanellis.skyspark.api.algorithms.factories;

import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.BlockNestedLoop;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SortFilterSkyline;
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

    @Override
    public SkylineAlgorithm getSortFilterSkyline(SparkContextWrapper sparkContext) {
        return new SortFilterSkyline(sparkContext);
    }
}
