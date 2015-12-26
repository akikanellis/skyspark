package com.github.dkanellis.skyspark.api.utils.point;

import java.io.Serializable;
import java.util.Comparator;

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
