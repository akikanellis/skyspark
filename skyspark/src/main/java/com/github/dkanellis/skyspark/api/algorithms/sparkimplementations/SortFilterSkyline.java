package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.algorithms.templates.BlockNestedLoopTemplate;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.PointFlag;
import com.github.dkanellis.skyspark.api.math.point.PointUtils;
import com.github.dkanellis.skyspark.api.math.point.comparators.DominationComparator;
import org.apache.spark.api.java.JavaPairRDD;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class SortFilterSkyline extends BlockNestedLoopTemplate {

    public SortFilterSkyline(SparkContextWrapper sparkContext) {
        super(sparkContext);
    }

    @Override
    protected JavaPairRDD<PointFlag, Point2D> sortRDD(
            JavaPairRDD<PointFlag, Point2D> flagPointPairs) {

        JavaPairRDD<Point2D, PointFlag> swapped = flagPointPairs.mapToPair(fp -> fp.swap());
        JavaPairRDD<Point2D, PointFlag> sorted = swapped.sortByKey(new DominationComparator());
        JavaPairRDD<PointFlag, Point2D> unswapped = sorted.mapToPair(fp -> fp.swap());
        return unswapped;
    }

    @Override
    protected void globalAddDiscardOrDominate(
            List<Point2D> globalSkylines, Point2D candidateGlobalSkylinePoint) {

        if (!isDominatedBySkylines(globalSkylines, candidateGlobalSkylinePoint)) {
            globalSkylines.add(candidateGlobalSkylinePoint);
        }
    }

    private boolean isDominatedBySkylines(List<Point2D> skylines, Point2D candidateSkylinePoint) {
        for (Point2D skyline : skylines) {
            if (PointUtils.dominates(skyline, candidateSkylinePoint)) {
                return true;
            }
        }
        return false;
    }
}
