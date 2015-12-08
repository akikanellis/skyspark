package com.github.dkanellis.skyspark.performance.result;

import javax.validation.constraints.NotNull;

import static com.github.dkanellis.skyspark.api.algorithms.Preconditions.checkNotEmpty;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dimitris Kanellis
 */
public class Result {

    private final String algorithmName;
    private final PointDataFile pointDataFile;
    private final long elapsedTime;

    public Result(@NotNull String algorithmName, @NotNull PointDataFile pointDataFile, final long elapsedTime) {
        this.algorithmName = checkNotEmpty(algorithmName);
        this.pointDataFile = checkNotNull(pointDataFile);
        checkArgument(elapsedTime > 0);
        this.elapsedTime = elapsedTime;
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

    public long getElapsedTime() {
        return elapsedTime;
    }
}
