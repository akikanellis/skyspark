package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.algorithms.templates.BlockNestedLoopTemplate;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.*;
import com.github.dkanellis.skyspark.api.math.point.*;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.*;

/**
 *
 * @author Dimitris Kanellis
 */
public class BlockNestedLoop extends BlockNestedLoopTemplate {

    public BlockNestedLoop(SparkContextWrapper sparkContext) {
        super(sparkContext);
    }

    @Override
    protected JavaPairRDD<PointFlag, Point2DAdvanced> sortRDD(JavaPairRDD<PointFlag, Point2DAdvanced> flagPointPairs) {
        return flagPointPairs;
    }

    @Override
    protected void globalAddDiscardOrDominate(List<Point2DAdvanced> globalSkylines, Point2DAdvanced candidateGlobalSkylinePoint) {
        for (Iterator it = globalSkylines.iterator(); it.hasNext();) {
            Point2DAdvanced skyline = (Point2DAdvanced) it.next();
            if (skyline.dominates(candidateGlobalSkylinePoint)) {
                return;
            } else if (candidateGlobalSkylinePoint.dominates(skyline)) {
                it.remove();
            }
        }
        globalSkylines.add(candidateGlobalSkylinePoint);
    }

}
