package com.akikanellis.skyspark.api.algorithms.bnl;

import com.akikanellis.skyspark.api.algorithms.OldSkylineAlgorithm;
import com.akikanellis.skyspark.api.utils.point.Points;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The BlockNestedLoopTemplate is used by {@link OldBlockNestedLoop} and {@link com.akikanellis.skyspark.api.algorithms.sfs.SortFilterSkyline}
 * and it contains the main functionality of the BlockNestedLoopAlgorithm. First we do the division job of the algorithm
 * to <PointWithFlag, LocalSkylines> and then we merge the skylines together and calculate the total skyline set.
 */
public abstract class BlockNestedLoopTemplate implements OldSkylineAlgorithm {

    /**
     * Given a point and a median this produces a flag-point pair for the given point.
     */
    private OlgFlagPointPairProducer flagPointPairProducer;

    private static Point2D getMedianPointFromRDD(JavaRDD<Point2D> points) {
        Point2D biggestPointByXDimension = points.reduce(Points::getBiggestPointByXDimension);
        Point2D biggestPointByYDimension = points.reduce(Points::getBiggestPointByYDimension);

        double xDimensionMedian = biggestPointByXDimension.getX() / 2.0;
        double yDimensionMedian = biggestPointByYDimension.getY() / 2.0;

        return new Point2D.Double(xDimensionMedian, yDimensionMedian);
    }

    @Override
    public final JavaRDD<Point2D> computeSkylinePoints(JavaRDD<Point2D> points) {
        flagPointPairProducer = createFlagPointPairProducer(points);

        JavaPairRDD<OldPointFlag, Iterable<Point2D>> localSkylinePointsByFlag = divide(points);
        JavaRDD<Point2D> skylines = merge(localSkylinePointsByFlag);

        return skylines;
    }

    private OlgFlagPointPairProducer createFlagPointPairProducer(JavaRDD<Point2D> points) {
        Point2D medianPoint = getMedianPointFromRDD(points);
        return new OlgFlagPointPairProducer(medianPoint);
    }

    private JavaPairRDD<OldPointFlag, Iterable<Point2D>> divide(JavaRDD<Point2D> points) {
        JavaPairRDD<OldPointFlag, Point2D> flagPointPairs = points.mapToPair(p -> flagPointPairProducer.getFlagPointPair(p));
        JavaPairRDD<OldPointFlag, Iterable<Point2D>> pointsGroupedByFlag = flagPointPairs.groupByKey();
        JavaPairRDD<OldPointFlag, Iterable<Point2D>> flagsWithLocalSkylines
                = pointsGroupedByFlag.mapToPair(fp -> new Tuple2<>(fp._1(), getLocalSkylinesWithBnl(fp._2())));

        return flagsWithLocalSkylines;
    }

    private Iterable<Point2D> getLocalSkylinesWithBnl(Iterable<Point2D> pointIterable) {
        List<Point2D> localSkylines = new ArrayList<>();
        for (Point2D point : pointIterable) {
            localAddDiscardOrDominate(localSkylines, point);
        }
        return localSkylines;
    }

    protected void localAddDiscardOrDominate(List<Point2D> localSkylines, Point2D candidateSkylinePoint) {
        for (Iterator it = localSkylines.iterator(); it.hasNext(); ) {
            Point2D pointToCheckAgainst = (Point2D) it.next();
            if (Points.dominates(pointToCheckAgainst, candidateSkylinePoint)) {
                return;
            } else if (Points.dominates(candidateSkylinePoint, pointToCheckAgainst)) {
                it.remove();
            }
        }
        localSkylines.add(candidateSkylinePoint);
    }

    protected JavaRDD<Point2D> merge(JavaPairRDD<OldPointFlag, Iterable<Point2D>> localSkylinesGroupedByFlag) {
        JavaPairRDD<OldPointFlag, Point2D> ungroupedLocalSkylines = localSkylinesGroupedByFlag.flatMapValues(point -> point);
        JavaPairRDD<OldPointFlag, Point2D> sortedLocalSkylines = sortRdd(ungroupedLocalSkylines);

        JavaRDD<List<Tuple2<OldPointFlag, Point2D>>> groupedByTheSameId = groupByTheSameId(sortedLocalSkylines);
        JavaRDD<Point2D> globalSkylinePoints = groupedByTheSameId.flatMap(this::getGlobalSkylineWithBNLAndPrecomparisson);

        return globalSkylinePoints;
    }

    protected abstract JavaPairRDD<OldPointFlag, Point2D> sortRdd(JavaPairRDD<OldPointFlag, Point2D> flagPointPairs);

    private JavaRDD<List<Tuple2<OldPointFlag, Point2D>>> groupByTheSameId(JavaPairRDD<OldPointFlag, Point2D> ungroupedLocalSkylines) {
        JavaPairRDD<OldPointFlag, Point2D> mergedInOnePartition = ungroupedLocalSkylines.coalesce(1);
        JavaRDD<List<Tuple2<OldPointFlag, Point2D>>> groupedByTheSameId = mergedInOnePartition.glom();
        return groupedByTheSameId;
    }

    private List<Point2D> getGlobalSkylineWithBNLAndPrecomparisson(List<Tuple2<OldPointFlag, Point2D>> flagPointPairs) {
        List<Point2D> globalSkylines = new ArrayList<>();
        for (Tuple2<OldPointFlag, Point2D> flagPointPair : flagPointPairs) {
            OldPointFlag flag = flagPointPair._1();
            if (!passesPreComparisson(flag)) {
                continue;
            }

            Point2D point = flagPointPair._2();
            globalAddDiscardOrDominate(globalSkylines, point);
        }
        return globalSkylines;
    }

    private boolean passesPreComparisson(OldPointFlag flagToCheck) {
        OldPointFlag rejectedFlag = new OldPointFlag(1, 1);
        return !flagToCheck.equals(rejectedFlag);
    }

    protected abstract void globalAddDiscardOrDominate(
            List<Point2D> globalSkylines, Point2D candidateGlobalSkylinePoint);
}
