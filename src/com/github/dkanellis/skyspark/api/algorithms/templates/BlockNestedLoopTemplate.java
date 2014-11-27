package com.github.dkanellis.skyspark.api.algorithms.templates;

import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.TextFileToPointRDD;
import com.github.dkanellis.skyspark.api.math.point.FlagPointPairProducer;
import com.github.dkanellis.skyspark.api.math.point.MedianPointFinder;
import com.github.dkanellis.skyspark.api.math.point.Point2DAdvanced;
import com.github.dkanellis.skyspark.api.math.point.PointFlag;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 *
 * @author Dimitris Kanellis
 */
public abstract class BlockNestedLoopTemplate implements SkylineAlgorithm, Serializable {

    private final transient TextFileToPointRDD txtToPoints;
    private FlagPointPairProducer flagPointPairProducer;

    public BlockNestedLoopTemplate(SparkContextWrapper sparkContext) {
        this.txtToPoints = new TextFileToPointRDD(sparkContext);
    }

    @Override
    public final List<Point2DAdvanced> getSkylinePoints(String filePath) {
        JavaRDD<Point2DAdvanced> points = txtToPoints.getPointRDDFromTextFile(filePath, " ");
        flagPointPairProducer = createFlagPointPairProducer(points);

        JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> localSkylinePointsByFlag = divide(points);
        JavaRDD<Point2DAdvanced> skylinePoints = merge(localSkylinePointsByFlag);

        return skylinePoints.collect();
    }

    private FlagPointPairProducer createFlagPointPairProducer(JavaRDD<Point2DAdvanced> points) {
        Point2DAdvanced medianPoint = MedianPointFinder.findMedianPoint(points);
        return new FlagPointPairProducer(medianPoint);
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

    protected JavaRDD<Point2DAdvanced> merge(
            JavaPairRDD<PointFlag, Iterable<Point2DAdvanced>> localSkylinesGroupedByFlag) {

        JavaPairRDD<PointFlag, Point2DAdvanced> ungroupedLocalSkylines
                = localSkylinesGroupedByFlag.flatMapValues(point -> point);
        JavaPairRDD<PointFlag, Point2DAdvanced> sortedLocalSkylines = sortRDD(ungroupedLocalSkylines);

        JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupedByTheSameId = groupByTheSameId(sortedLocalSkylines);
        JavaRDD<Point2DAdvanced> globalSkylinePoints
                = groupedByTheSameId.flatMap(singleList -> getGlobalSkylineWithBNLAndPrecomparisson(singleList));

        return globalSkylinePoints;
    }

    protected abstract JavaPairRDD<PointFlag, Point2DAdvanced> sortRDD(JavaPairRDD<PointFlag, Point2DAdvanced> flagPointPairs);

    private JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupByTheSameId(JavaPairRDD<PointFlag, Point2DAdvanced> ungroupedLocalSkylines) {
        JavaPairRDD<PointFlag, Point2DAdvanced> mergedInOnePartition = ungroupedLocalSkylines.coalesce(1);
        JavaRDD<List<Tuple2<PointFlag, Point2DAdvanced>>> groupedByTheSameId = mergedInOnePartition.glom();
        return groupedByTheSameId;
    }

    private List<Point2DAdvanced> getGlobalSkylineWithBNLAndPrecomparisson(List<Tuple2<PointFlag, Point2DAdvanced>> flagPointPairs) {
        List<Point2DAdvanced> globalSkylines = new ArrayList<>();
        for (Tuple2<PointFlag, Point2DAdvanced> flagPointPair : flagPointPairs) {
            PointFlag flag = flagPointPair._1;
            if (!passesPreComparisson(flag)) {
                continue;
            }

            Point2DAdvanced point = flagPointPair._2;
            globalAddDiscardOrDominate(globalSkylines, point);
        }
        return globalSkylines;
    }

    private boolean passesPreComparisson(PointFlag flagToCheck) {
        PointFlag rejectedFlag = new PointFlag(1, 1);
        return !flagToCheck.equals(rejectedFlag);
    }

    protected abstract void globalAddDiscardOrDominate(
            List<Point2DAdvanced> globalSkylines, Point2DAdvanced candidateGlobalSkylinePoint);
}
