package com.github.dkanellis.skyspark.performance.result;

import javax.validation.constraints.NotNull;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Result {

    private final String algorithmName;
    private final PointDataFile pointDataFile;
    private final long elapsedTime;
    private final int numberOfSkylines;

    public Result(@NotNull String algorithmName, @NotNull PointDataFile pointDataFile,
                  final long elapsedTime, final int numberOfSkylines) {
        checkArgument(elapsedTime > 0);
        this.algorithmName = checkNotEmpty(algorithmName);
        this.pointDataFile = checkNotNull(pointDataFile);
        this.numberOfSkylines = numberOfSkylines;
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

    public int getNumberOfSkylines() {
        return numberOfSkylines;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }
}
