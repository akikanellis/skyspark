package com.github.dkanellis.skyspark.api.algorithms;


import javax.annotation.Nullable;

public final class Preconditions {

    private Preconditions() {
        throw new AssertionError("No instances.");
    }

    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }

    public static String checkNotEmpty(@Nullable final String input) {
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("String is empty");
        }

        return input;
    }

    public static int checkInBounds(final int value, final int lowBound, final int highBound) {
        if (value < lowBound) {
            throw new IllegalArgumentException(String.format(
                    "Value must not be smaller than lower bound. Value: %d, Lower bound: %d", value, lowBound));
        }

        if (value > highBound) {
            throw new IllegalArgumentException(String.format(
                    "Value must not be bigger than higher bound. Value: %d, Higher bound: %d", value, highBound));
        }

        return value;
    }

    public static int checkNotNegativeOrZero(final int number) {
        if (number <= 0) {
            throw new IllegalArgumentException("Number must not be negative or zero: " + number);
        }

        return number;
    }

    public static int checkNotPositiveOrZero(final int number) {
        if (number >= 0) {
            throw new IllegalArgumentException("Number must not be positive or zero: " + number);
        }

        return number;
    }
}
