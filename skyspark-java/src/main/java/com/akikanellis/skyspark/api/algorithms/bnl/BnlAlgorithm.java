package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.Point;
import com.google.common.annotations.VisibleForTesting;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@VisibleForTesting
public class BnlAlgorithm implements Serializable {

    Iterable<Point> computeSkylinesWithoutPreComparison(Iterable<Point> points) {
        List<Point> skylines = new ArrayList<>();

        points.forEach(p -> addDiscardOrDominate(skylines, p));

        return skylines;
    }

    private void addDiscardOrDominate(List<Point> currentSkylines, Point candidateSkyline) {
        Iterator<Point> currentSkylinesIterator = currentSkylines.iterator();
        while (currentSkylinesIterator.hasNext()){
            Point pointToCheckAgainst = currentSkylinesIterator.next();

            if (pointToCheckAgainst.dominates(candidateSkyline)) return;

            if (candidateSkyline.dominates(pointToCheckAgainst)) currentSkylinesIterator.remove();
        }

        currentSkylines.add(candidateSkyline);
    }

    Iterable<Point> computeSkylinesWithPreComparison(Collection<Tuple2<Flag, Point>> flagsPoints) {
        List<Point> skylines = new ArrayList<>();

        flagsPoints.stream()
                .filter(fp -> passesPreComparison(fp._1()))
                .map(Tuple2::_2)
                .forEach(p -> addDiscardOrDominate(skylines, p));

        return skylines;
    }

    private boolean passesPreComparison(Flag flag) {
        for (int i = 0; i < flag.size(); i++) {
            if (!flag.bit(i)) return true;
        }

        return false;
    }
}
