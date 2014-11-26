package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

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
public class SparkBootNestedLoop implements SkylineAlgorithm, Serializable {

    private final transient TextFileToPointRDD txtToPoints;
    private FlagPointPairProducer flagPointPairProducer;

    public SparkBootNestedLoop(SparkContextWrapper sparkContext) {
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
        JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> pointsGroupedByFlag = flagPointPairs.groupByKey();
        JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> flagsWithLocalSkylines
                = pointsGroupedByFlag.mapToPair(fp -> new Tuple2(fp._1, getLocalSkylinesWithBNL(fp._2)));

        return flagsWithLocalSkylines;
    }

    private Iterable<Point2DAdvanced> getLocalSkylinesWithBNL(Iterable<Point2DAdvanced> pointIterable) {
        List<Point2DAdvanced> localSkylines = new ArrayList<>();
        for (Point2DAdvanced point : pointIterable) {
            localAddDiscardOrDominate(localSkylines, point);
        }
        return localSkylines;
    }

    private void localAddDiscardOrDominate(List<Point2DAdvanced> localSkylines, Point2DAdvanced candidateSkylinePoint) {
        for (Iterator it = localSkylines.iterator(); it.hasNext();) {
            Point2DAdvanced pointToCheckAgainst = (Point2DAdvanced) it.next();
            if (pointToCheckAgainst.dominates(candidateSkylinePoint)) {
                return;
            } else if (candidateSkylinePoint.dominates(pointToCheckAgainst)) {
                it.remove();
            }
        }
        localSkylines.add(candidateSkylinePoint);
    }

    private JavaRDD<Point2DAdvanced> merge(JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> localSkylinesGroupedByFlag) {
        JavaPairRDD<PointFlag, Point2DAdvanced> ungroupedLocalSkylines
                = localSkylinesGroupedByFlag.flatMapValues(point -> point);
        
        JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupedByTheSameId = groupByTheSameId(ungroupedLocalSkylines);
        JavaRDD<Point2DAdvanced> globalSkylinePoints
                = groupedByTheSameId.flatMap(singleList -> getGlobalSkylineWithBNLAndPrecomparisson(singleList));

        return globalSkylinePoints;
    }

    private JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupByTheSameId(JavaPairRDD<PointFlag, Point2DAdvanced> ungroupedLocalSkylines) {
        JavaPairRDD<PointFlag, Point2DAdvanced> mergedInOnePartition = ungroupedLocalSkylines.coalesce(1);
        JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupedByTheSameId = mergedInOnePartition.glom();
        return groupedByTheSameId;
    }

    private List<Point2DAdvanced> getGlobalSkylineWithBNLAndPrecomparisson(List<Tuple2<PointFlag, Point2DAdvanced>> flagPointPairs) {
        List<Point2DAdvanced> globalSkylines = new ArrayList<>();
        for (Tuple2<PointFlag, Point2DAdvanced> flagPointPair : flagPointPairs) {
            globalAddDiscardOrDominate(globalSkylines, flagPointPair);
        }
        return globalSkylines;
    }

    private void globalAddDiscardOrDominate(List<Point2DAdvanced> globalSkylines, Tuple2<PointFlag, Point2DAdvanced> flagPointPair) {
        PointFlag dominatedSubspace = new PointFlag(1, 1);
        PointFlag pointFlag = flagPointPair._1;
        if (pointFlag.equals(dominatedSubspace)) {
            return;
        }

        Point2DAdvanced candidateGlobalSkylinePoint = flagPointPair._2;
        for (Iterator it = globalSkylines.iterator(); it.hasNext();) {
            Point2DAdvanced pointToCheckAgainst = (Point2DAdvanced) it.next();
            if (pointToCheckAgainst.dominates(candidateGlobalSkylinePoint)) {
                return;
            } else if (candidateGlobalSkylinePoint.dominates(pointToCheckAgainst)) {
                it.remove();
            }
        }
        globalSkylines.add(candidateGlobalSkylinePoint);
    }
}
