package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.awt.geom.Point2D;
import java.util.BitSet;

class PointsWithRequiredBitSlices {

    private final Point2D point;

    private final BitSet firstDimensionCurrentBitSlice;
    private final BitSet secondDimensionCurrentBitSlice;

    private final BitSet firstDimensionPreviousBitSlice;
    private final BitSet secondDimensionPreviousBitSlice;

    public PointsWithRequiredBitSlices(@NotNull Point2D point,
                                       @NotNull BitSlices firstDimensionRequiredBitSlices,
                                       @NotNull BitSlices secondDimensionRequiredBitSlices) {
        this.point = point;

        this.firstDimensionCurrentBitSlice = firstDimensionRequiredBitSlices.getFirstSlice();
        this.secondDimensionCurrentBitSlice = secondDimensionRequiredBitSlices.getFirstSlice();

        this.firstDimensionPreviousBitSlice = firstDimensionRequiredBitSlices.getSecondSlice();
        this.secondDimensionPreviousBitSlice = secondDimensionRequiredBitSlices.getSecondSlice();
    }

    public static PointsWithRequiredBitSlices fromTuple(Tuple2<Point2D, Tuple2<BitSlices, BitSlices>> tuple) {
        return new PointsWithRequiredBitSlices(tuple._1(),
                tuple._2()._1(),
                tuple._2()._2());
    }

    Point2D getPoint() {
        return point;
    }

    boolean isSkyline() {
        BitSet A = (BitSet) firstDimensionCurrentBitSlice.clone();
        A.and(secondDimensionCurrentBitSlice);

        BitSet B = (BitSet) firstDimensionPreviousBitSlice.clone();
        B.or(secondDimensionPreviousBitSlice);

        BitSet C = (BitSet) A.clone();
        C.and(B);

        return C.isEmpty();
    }
}
