package com.github.dkanellis.skyspark.performance;

import org.apache.spark.SparkConf;

import javax.validation.constraints.NotNull;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;

public final class SparkConfs {

    private SparkConfs() {
        throw new AssertionError("No instances.");
    }

    public static double getSlaveMemory(@NotNull SparkConf sparkConf) {
        String memory = sparkConf.get("spark.executors.memory", "0m");
        return memoryStringToGigabytes(memory);
    }

    public static double getMasterMemory(@NotNull SparkConf sparkConf) {
        String memory = sparkConf.get("spark.driver.memory", "0m");
        return memoryStringToGigabytes(memory);
    }

    public static double memoryStringToGigabytes(String memory) {
        checkNotEmpty(memory);

        String amountString = memory.substring(0, memory.length() - 1);
        double amount = Double.parseDouble(amountString);

        if (memory.endsWith("g") || memory.endsWith("gb")) {
            return amount;
        } else if (memory.endsWith("m") || memory.endsWith("mb")) {
            return amount / 1024.0;
        } else {
            return 0;
        }
    }
}
