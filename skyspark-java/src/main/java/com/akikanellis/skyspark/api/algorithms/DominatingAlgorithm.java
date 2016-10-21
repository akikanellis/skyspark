package com.akikanellis.skyspark.api.algorithms;

class DominatingAlgorithm {

    static boolean dominates(Point p, Point q) {
        boolean atLeastOneDimensionIsSmaller = false;

        for (int i = 0; i < p.size(); i++) {
            double candidateDimension = p.dimension(i);
            double otherDimension = q.dimension(i);

            if (candidateDimensionIsWorse(candidateDimension, otherDimension)) return false;

            if (candidateDimensionIsBetter(candidateDimension, otherDimension)) atLeastOneDimensionIsSmaller = true;
        }

        return atLeastOneDimensionIsSmaller;
    }

    private static boolean candidateDimensionIsWorse(double candidateDimension, double otherDimension) {
        return candidateDimension > otherDimension;
    }

    private static boolean candidateDimensionIsBetter(double candidateDimension, double otherDimension) {
        return candidateDimension < otherDimension;
    }
}
