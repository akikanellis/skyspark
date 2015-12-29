package com.github.dkanellis.skyspark.performance.result;

import org.apache.spark.SparkConf;

import javax.validation.constraints.NotNull;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Result {

    private final String algorithmName;
    private final PointDataFile pointDataFile;
    private final long elapsedTime;
    private final long numberOfSkylines;
    private final int numberOfSlaves;
    private final int numberOfCoresPerSlave;
    private final double masterMemory;
    private final double slaveMemory;

    public Result(@NotNull String algorithmName, @NotNull PointDataFile pointDataFile,
                  final long elapsedTime, final int numberOfSkylines) {
        this(algorithmName, pointDataFile, elapsedTime, numberOfSkylines, 0, 0, 0, 0);
    }

    public Result(@NotNull String algorithmName, @NotNull PointDataFile pointDataFile,
                  final long elapsedTime, final long numberOfSkylines, final int numberOfSlaves, @NotNull SparkConf sparkConf) {
        this(algorithmName, pointDataFile, elapsedTime, numberOfSkylines, numberOfSlaves,
                sparkConf.getInt("spark.executor.cores", 0),
                sparkConf.getInt("spark.driver.memory", 0),
                sparkConf.getInt("spark.executor.memory", 0));
    }

    public Result(@NotNull String algorithmName, @NotNull PointDataFile pointDataFile, final long elapsedTime,
                  final long numberOfSkylines, final int numberOfSlaves, final int numberOfCoresPerSlave,
                  final double masterMemory, final double slaveMemory) {
        checkArgument(elapsedTime > 0);
        this.algorithmName = checkNotEmpty(algorithmName);
        this.pointDataFile = checkNotNull(pointDataFile);
        this.numberOfSkylines = numberOfSkylines;
        this.elapsedTime = elapsedTime;
        this.numberOfSlaves = numberOfSlaves;
        this.numberOfCoresPerSlave = numberOfCoresPerSlave;
        this.masterMemory = masterMemory;
        this.slaveMemory = slaveMemory;
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

    public long getNumberOfSkylines() {
        return numberOfSkylines;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public int getNumberOfSlaves() {
        return numberOfSlaves;
    }

    public int getNumberOfCoresPerSlave() {
        return numberOfCoresPerSlave;
    }

    public double getMasterMemory() {
        return masterMemory;
    }

    public double getSlaveMemory() {
        return slaveMemory;
    }
}
