package com.akikanellis.skyspark.api.algorithms.bnl;

import java.util.Arrays;

public final class Flag {
    private final boolean[] bits;

    public Flag(boolean... bits) { this.bits = bits; }

    public int size() { return bits.length; }

    public boolean bit(int i) { return bits[i]; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Flag other = (Flag) o;
        return Arrays.equals(bits, other.bits);
    }

    @Override public int hashCode() { return Arrays.hashCode(bits); }

    @Override public String toString() {
        String values = Arrays.toString(bits)
                .replace('[', '(')
                .replace(']', ')')
                .replace("true", "1")
                .replace("false", "0")
                .replace(", ", "");
        return "Flag" + values;
    }
}
