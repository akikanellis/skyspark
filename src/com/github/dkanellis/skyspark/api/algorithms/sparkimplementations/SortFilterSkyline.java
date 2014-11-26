package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.TextFileToPointRDD;
import com.github.dkanellis.skyspark.api.math.point.*;
import com.github.dkanellis.skyspark.api.math.point.comparators.DominationComparator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 *
 * @author Dimitris Kanellis
 */
public class SortFilterSkyline implements SkylineAlgorithm, Serializable {

    private final transient TextFileToPointRDD txtToPoints;
    private FlagPointPairProducer flagPointPairProducer;

    public SortFilterSkyline(SparkContextWrapper sparkContext) {
        this.txtToPoints = new TextFileToPointRDD(sparkContext);
    }

    @Override
    public List<Point2DAdvanced> getSkylinePoints(String filePath) {
        JavaRDD<Point2DAdvanced> points = txtToPoints.getPointRDDFromTextFile(filePath, " ");

        createFlagPointPairProducer(points);

        JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> localSkylinePointsByFlag = divide(points);
        JavaRDD<Point2DAdvanced> skylinePoints = merge(localSkylinePointsByFlag);

        return skylinePoints.collect();
    }

    private void createFlagPointPairProducer(JavaRDD<Point2DAdvanced> points) {
        Point2DAdvanced medianPoint = MedianPointFinder.findMedianPoint(points);
        this.flagPointPairProducer = new FlagPointPairProducer(medianPoint);
    }

    private JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> divide(JavaRDD<Point2DAdvanced> points) {
        JavaPairRDD<PointFlag, Point2DAdvanced> flagPointPairs = points.mapToPair(p -> flagPointPairProducer.getFlagPointPair(p));
        JavaPairRDD<PointFlag, Point2DAdvanced> sortedFlagPointPairs = sortByValue(flagPointPairs);

        JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> pointsGroupedByFlag = sortedFlagPointPairs.groupByKey();
        JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> flagsWithLocalSkylines
                = pointsGroupedByFlag.mapToPair(fp -> new Tuple2(fp._1, getLocalSkylinesWithPresortedBNL(fp._2)));

        return flagsWithLocalSkylines;
    }

    private JavaPairRDD<PointFlag, Point2DAdvanced> sortByValue(JavaPairRDD<PointFlag, Point2DAdvanced> flagPointPairs) {
        JavaPairRDD<Point2DAdvanced, PointFlag> swapped = flagPointPairs.mapToPair(fp -> fp.swap());
        JavaPairRDD<Point2DAdvanced, PointFlag> sorted = swapped.sortByKey(new DominationComparator());
        JavaPairRDD<PointFlag, Point2DAdvanced> unswapped = sorted.mapToPair(fp -> fp.swap());
        return unswapped;
    }

    private Iterable<Point2DAdvanced> getLocalSkylinesWithPresortedBNL(Iterable<Point2DAdvanced> pointIterable) {
        List<Point2DAdvanced> localSkylines = new ArrayList<>();
        for (Point2DAdvanced point : pointIterable) {
            if (!isDominatedBySkylines(localSkylines, point)) {
                localSkylines.add(point);
            }
        }
        return localSkylines;
    }

    private boolean isDominatedBySkylines(List<Point2DAdvanced> skylines, Point2DAdvanced candidateSkylinePoint) {
        for (Point2DAdvanced skyline : skylines) {
            if (skyline.dominates(candidateSkylinePoint)) {
                return true;
            }
        }
        return false;
    }

    private JavaRDD<Point2DAdvanced> merge(JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> localSkylinesGroupedByFlag) {
        JavaPairRDD<PointFlag, Point2DAdvanced> ungroupedLocalSkylines = localSkylinesGroupedByFlag.flatMapValues(point -> point);
        JavaPairRDD<PointFlag, Point2DAdvanced> sortedLocalSkylines = sortByValue(ungroupedLocalSkylines);
        JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupedByTheSameId = groupByTheSameId(sortedLocalSkylines);
        JavaRDD<Point2DAdvanced> globalSkylines
                = groupedByTheSameId.flatMap(singleList -> getGlobalSkylineWithPresortedBNLAndPrecomparisson(singleList));

        return globalSkylines;
    }

    private JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupByTheSameId(JavaPairRDD<PointFlag, Point2DAdvanced> ungroupedLocalSkylines) {
        JavaPairRDD<PointFlag, Point2DAdvanced> mergedInOnePartition = ungroupedLocalSkylines.coalesce(1);
        JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupedByTheSameId = mergedInOnePartition.glom();
        return groupedByTheSameId;
    }

    private List<Point2DAdvanced> getGlobalSkylineWithPresortedBNLAndPrecomparisson(List<Tuple2<PointFlag, Point2DAdvanced>> flagPointPairs) {
        List<Point2DAdvanced> globalSkylines = new ArrayList<>();
        for (Tuple2<PointFlag, Point2DAdvanced> flagPointPair : flagPointPairs) {
            PointFlag flag = flagPointPair._1;
            Point2DAdvanced point = flagPointPair._2;

            if (passesPreComparisson(flag) && !isDominatedBySkylines(globalSkylines, point)) {
                globalSkylines.add(point);
            }
        }
        return globalSkylines;
    }

    private boolean passesPreComparisson(PointFlag flagToCheck) {
        PointFlag rejectedFlag = new PointFlag(1, 1);
        return !flagToCheck.equals(rejectedFlag);
    }
}
