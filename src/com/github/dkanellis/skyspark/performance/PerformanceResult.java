package com.github.dkanellis.skyspark.performance;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class PerformanceResult {

    private final String algorithmName;
    private final File inputFile;
    private List<Long> durations;
    private int timesRun;

    public PerformanceResult(String algorithmName, File fileName) {
        this.algorithmName = algorithmName;
        this.inputFile = fileName;
        this.durations = new ArrayList<>();
        this.timesRun = 0;
    }

    public void addResult(long duration) {
        durations.add(duration);
        timesRun++;
    }

    public double getAverageTime() {
        long totalDuration = 0;
        for (Long duration : durations) {
            totalDuration += duration;
        }
        return (double) totalDuration / timesRun;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public File getFileName() {
        return inputFile;
    }

    public int getTimesRun() {
        return timesRun;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Algorithm: ").append(algorithmName).append("\n");
        builder.append("File: ").append(inputFile).append("\n");
        builder.append("Times algorithm run: ").append(timesRun).append("\n");
        builder.append("Time (average): ").append(getAverageTime()).append("\n");
        return builder.toString();
    }
}
