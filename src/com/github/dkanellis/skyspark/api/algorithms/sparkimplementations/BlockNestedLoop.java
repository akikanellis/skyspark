package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.algorithms.templates.BlockNestedLoopTemplate;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.PointFlag;
import com.github.dkanellis.skyspark.api.math.point.PointUtils;
import org.apache.spark.api.java.JavaPairRDD;

import java.awt.geom.Point2D;
import java.util.Iterator;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class BlockNestedLoop extends BlockNestedLoopTemplate {

    public BlockNestedLoop(SparkContextWrapper sparkContext) {
        super(sparkContext);
    }

    @Override
    protected JavaPairRDD<PointFlag, Point2D> sortRDD(JavaPairRDD<PointFlag, Point2D> flagPointPairs) {
        return flagPointPairs;
    }

    @Override
    protected void globalAddDiscardOrDominate(List<Point2D> globalSkylines, Point2D candidateGlobalSkylinePoint) {
        for (Iterator it = globalSkylines.iterator(); it.hasNext(); ) {
            Point2D skyline = (Point2D) it.next();
            if (PointUtils.dominates(skyline, candidateGlobalSkylinePoint)) {
                return;
            } else if (PointUtils.dominates(candidateGlobalSkylinePoint, skyline)) {
                it.remove();
            }
        }
        globalSkylines.add(candidateGlobalSkylinePoint);
    }

}
