package com.github.dkanellis.skyspark.api.algorithms.sfs;

import com.github.dkanellis.skyspark.api.algorithms.bnl.BlockNestedLoopTemplate;
import com.github.dkanellis.skyspark.api.algorithms.bnl.PointFlag;
import com.github.dkanellis.skyspark.api.utils.point.DominationComparator;
import com.github.dkanellis.skyspark.api.utils.point.Points;
import org.apache.spark.api.java.JavaPairRDD;

import java.awt.geom.Point2D;
import java.util.List;

public class SortFilterSkyline extends BlockNestedLoopTemplate {

    @Override
    protected JavaPairRDD<PointFlag, Point2D> sortRdd(
            JavaPairRDD<PointFlag, Point2D> flagPointPairs) {

        JavaPairRDD<Point2D, PointFlag> swapped = flagPointPairs.mapToPair(fp -> fp.swap());
        JavaPairRDD<Point2D, PointFlag> sorted = swapped.sortByKey(new DominationComparator());
        JavaPairRDD<PointFlag, Point2D> unswapped = sorted.mapToPair(fp -> fp.swap());
        return unswapped;
    }

    @Override
    protected void globalAddDiscardOrDominate(List<Point2D> globalSkylines, Point2D candidateGlobalSkylinePoint) {
        if (!isDominatedBySkylines(globalSkylines, candidateGlobalSkylinePoint)) {
            globalSkylines.add(candidateGlobalSkylinePoint);
        }
    }

    private boolean isDominatedBySkylines(List<Point2D> skylines, Point2D candidateSkylinePoint) {
        for (Point2D skyline : skylines) {
            if (Points.dominates(skyline, candidateSkylinePoint)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "SortFilterSkyline";
    }
}
