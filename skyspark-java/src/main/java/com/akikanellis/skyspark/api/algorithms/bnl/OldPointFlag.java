package com.akikanellis.skyspark.api.algorithms.bnl;

import java.io.Serializable;

/**
 * The OldPointFlag holds the two bits which signify where in the plane the point associated with the flag is located.
 */
public class OldPointFlag implements Serializable {

    private final int xBit;
    private final int yBit;

    public OldPointFlag(int xBit, int yBit) {
        this.xBit = xBit;
        this.yBit = yBit;
    }

    public int getXBit() {
        return xBit;
    }

    public int getYBit() {
        return yBit;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof OldPointFlag)) {
            return false;
        }

        OldPointFlag other = (OldPointFlag) o;
        return xBit == other.getXBit() && yBit == other.getYBit();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 59 * hash + xBit;
        hash = 59 * hash + yBit;
        return hash;
    }

    @Override
    public String toString() {
        return "[" + xBit + yBit + ']';
    }

}
