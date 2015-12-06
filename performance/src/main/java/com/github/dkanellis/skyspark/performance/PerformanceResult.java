package com.github.dkanellis.skyspark.performance;

import javax.validation.constraints.NotNull;

import static com.github.dkanellis.skyspark.api.algorithms.Preconditions.checkNotEmpty;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dimitris Kanellis
 */
public class PerformanceResult {

    private final String algorithmName;
    private final PointDataFile pointDataFile;

    public PerformanceResult(@NotNull String algorithmName, @NotNull PointDataFile pointDataFile) {
        this.algorithmName = checkNotEmpty(algorithmName);
        this.pointDataFile = checkNotNull(pointDataFile);
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public String getDataType() {
        return pointDataFile.getDataType();
    }

    public int getDataSize() {
        return pointDataFile.getDataSize();
    }
}
