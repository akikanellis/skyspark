package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import scala.Tuple2;

import java.util.Iterator;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

class FlagProducer {
    private final Point median;

    FlagProducer(Point median) { this.median = median; }

    Flag calculateFlag(Point point) {
        Boolean[] bitsBoxed = getBits(point);
        boolean[] bits = unbox(bitsBoxed);

        return new Flag(bits);
    }

    private Boolean[] getBits(Point point) {
        return zip(point.stream(), median.stream())
                .map(t -> isPointWorseThanMedian(t._1(), t._2()))
                .toArray(Boolean[]::new);
    }

    private static Stream<Tuple2<Double, Double>> zip(DoubleStream first, DoubleStream second) {
        Iterator<Double> i = first.iterator();
        return second
                .filter(x -> i.hasNext())
                .mapToObj(b -> new Tuple2<>(i.next(), b));
    }

    private boolean isPointWorseThanMedian(Double pointDimension, Double medianDimension) {
        return pointDimension >= medianDimension;
    }

    private boolean[] unbox(Boolean[] bitsBoxed) {
        boolean[] bits = new boolean[bitsBoxed.length];
        for (int i = 0; i < bitsBoxed.length; i++) {
            bits[i] = bitsBoxed[i];
        }
        return bits;
    }
}
