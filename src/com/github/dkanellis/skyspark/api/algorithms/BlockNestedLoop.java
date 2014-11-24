package com.github.dkanellis.skyspark.api.algorithms;

import com.github.dkanellis.skyspark.api.algorithms.wrappers.*;
import com.github.dkanellis.skyspark.api.math.point.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.*;
import scala.Tuple2;

/**
 *
 * @author Dimitris Kanellis
 */
public class BlockNestedLoop implements SkylineAlgorithm, Serializable {

    private final transient TextFileToPointRDD txtToPoints;
    private FlagPointPairProducer flagPointPairProducer;

    public BlockNestedLoop(SparkContextWrapper sparkContext) {
        this.txtToPoints = new TextFileToPointRDD(sparkContext);
    }

    @Override
    public List<Point> getSkylinePoints(String filePath) {
        JavaRDD<Point> points = txtToPoints.getPointRDDFromTextFile(filePath, " ");

        createFlagPointPairProducer(points);

        JavaPairRDD<PointFlag, Iterable<Point>> localSkylinePointsByFlag = divide(points);
        JavaRDD<Point> skylinePoints = merge(localSkylinePointsByFlag);

        return skylinePoints.collect();
    }

    private void createFlagPointPairProducer(JavaRDD<Point> points) {
        Point medianPoint = MedianPointFinder.findMedianPoint(points);
        this.flagPointPairProducer = new FlagPointPairProducer(medianPoint);
    }

    private JavaPairRDD<PointFlag, Iterable<Point>> divide(JavaRDD<Point> points) {
        JavaPairRDD<PointFlag, Point> flagPointPairs = points.mapToPair(p -> flagPointPairProducer.getFlagPointPair(p));
        JavaPairRDD<PointFlag, Iterable<Point>> pointsGroupedByFlag = flagPointPairs.groupByKey();
        JavaPairRDD<PointFlag, Iterable<Point>> flagsWithLocalSkylines
                = pointsGroupedByFlag.mapToPair(fp -> new Tuple2(fp._1, getLocalSkylinesWithBNL(fp._2)));

        return flagsWithLocalSkylines;
    }

    private Iterable<Point> getLocalSkylinesWithBNL(Iterable<Point> pointIterable) {
        List<Point> localSkylines = new ArrayList<>();
        for (Point point : pointIterable) {
            localAddDiscardOrDominate(localSkylines, point);
        }
        return localSkylines;
    }

    private void localAddDiscardOrDominate(List<Point> localSkylines, Point candidateSkylinePoint) {
        for (Iterator it = localSkylines.iterator(); it.hasNext();) {
            Point pointToCheckAgainst = (Point) it.next();
            if (pointToCheckAgainst.dominates(candidateSkylinePoint)) {
                return;
            } else if (candidateSkylinePoint.dominates(pointToCheckAgainst)) {
                it.remove();
            }
        }
        localSkylines.add(candidateSkylinePoint);
    }

    private JavaRDD<Point> merge(JavaPairRDD<PointFlag, Iterable<Point>> localSkylinesGroupedByFlag) {
        JavaPairRDD<PointFlag, Point> ungroupedLocalSkylines = localSkylinesGroupedByFlag.flatMapValues(point -> point);
        JavaRDD<List<Tuple2<PointFlag, Point>>> groupedByTheSameId = ungroupedLocalSkylines.glom();
        JavaRDD<Point> globalSkylinePoints
                = groupedByTheSameId.flatMap(singleList -> getGlobalSkylineWithBNLAndPrecomparisson(singleList));

        return globalSkylinePoints;
    }

    private List<Point> getGlobalSkylineWithBNLAndPrecomparisson(List<Tuple2<PointFlag, Point>> flagPointPairs) {
        List<Point> globalSkylines = new ArrayList<>();
        for (Tuple2<PointFlag, Point> flagPointPair : flagPointPairs) {
            globalAddDiscardOrDominate(globalSkylines, flagPointPair);
        }
        return globalSkylines;
    }

    private void globalAddDiscardOrDominate(List<Point> globalSkylines, Tuple2<PointFlag, Point> flagPointPair) {
        PointFlag dominatedSubspace = new PointFlag(1, 1);
        PointFlag pointFlag = flagPointPair._1;
        if (pointFlag.equals(dominatedSubspace)) {
            return;
        }

        Point candidateGlobalSkylinePoint = flagPointPair._2;
        for (Iterator it = globalSkylines.iterator(); it.hasNext();) {
            Point pointToCheckAgainst = (Point) it.next();
            if (pointToCheckAgainst.dominates(candidateGlobalSkylinePoint)) {
                return;
            } else if (candidateGlobalSkylinePoint.dominates(pointToCheckAgainst)) {
                it.remove();
            }
        }
        globalSkylines.add(candidateGlobalSkylinePoint);
    }
}
