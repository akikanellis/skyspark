package com.github.dkanellis.skyspark.api.algorithms;


import javax.annotation.Nullable;

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
