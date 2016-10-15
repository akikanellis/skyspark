package com.akikanellis.skyspark.api.utils.point;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A comparator which returns if a double value dominates another using the MIN annotation.
 */
public class DominationComparatorMinAnotation implements Comparator<Double>, Serializable {

    @Override
    public int compare(Double first, Double second) {
        if (first < second) {
            return -1;
        } else if (second < first) {
            return 1;
        } else {
            return 0;
        }
    }
}
