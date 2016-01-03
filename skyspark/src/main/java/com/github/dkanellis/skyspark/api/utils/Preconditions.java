package com.github.dkanellis.skyspark.api.utils;


import javax.annotation.Nullable;

/**
 * Collection of extra preconditions that are not included in the guava library.
 */
public final class Preconditions {

    private Preconditions() {
        throw new AssertionError("No instances.");
    }

    public static String checkNotEmpty(@Nullable final String input) {
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("String is empty");
        }

        return input;
    }
}
