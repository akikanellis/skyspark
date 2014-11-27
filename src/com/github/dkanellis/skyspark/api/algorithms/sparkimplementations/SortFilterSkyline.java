package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.algorithms.templates.BlockNestedLoopTemplate;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.*;
import com.github.dkanellis.skyspark.api.math.point.comparators.DominationComparator;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;

/**
 *
 * @author Dimitris Kanellis
 */
public class SortFilterSkyline extends BlockNestedLoopTemplate {

    public SortFilterSkyline(SparkContextWrapper sparkContext) {
        super(sparkContext);
    }

    @Override
    protected JavaPairRDD<PointFlag, Point2DAdvanced> sortRDD(
            JavaPairRDD<PointFlag, Point2DAdvanced> flagPointPairs) {

        JavaPairRDD<Point2DAdvanced, PointFlag> swapped = flagPointPairs.mapToPair(fp -> fp.swap());
        JavaPairRDD<Point2DAdvanced, PointFlag> sorted = swapped.sortByKey(new DominationComparator());
        JavaPairRDD<PointFlag, Point2DAdvanced> unswapped = sorted.mapToPair(fp -> fp.swap());
        return unswapped;
    }

    @Override
    protected void globalAddDiscardOrDominate(
            List<Point2DAdvanced> globalSkylines, Point2DAdvanced candidateGlobalSkylinePoint) {

        if (!isDominatedBySkylines(globalSkylines, candidateGlobalSkylinePoint)) {
            globalSkylines.add(candidateGlobalSkylinePoint);
        }
    }

    private boolean isDominatedBySkylines(List<Point2DAdvanced> skylines, Point2DAdvanced candidateSkylinePoint) {
        for (Point2DAdvanced skyline : skylines) {
            if (skyline.dominates(candidateSkylinePoint)) {
                return true;
            }
        }
        return false;
    }
}
