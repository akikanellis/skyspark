package com.akikanellis.skyspark.performance;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for the Java 8 date api.
 */
public final class Dates {
    private Dates() {
        throw new AssertionError("No instances.");
    }

    public static String nowFormatted() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd--HH-mm-ss");
        return LocalDateTime.now().format(formatter);
    }
}
