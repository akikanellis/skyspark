package com.github.dkanellis.skyspark.performance;

import org.apache.spark.SparkConf;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.github.dkanellis.skyspark.api.utils.Preconditions.checkNotEmpty;

/**
 * Utility class for {@link SparkConf}.
 */
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

        if (memory.endsWith("g")) {
            return round(amount, 2);
        } else if (memory.endsWith("m")) {
            return round(amount / 1024.0, 2);
        } else {
            return 0;
        }
    }

    private static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
