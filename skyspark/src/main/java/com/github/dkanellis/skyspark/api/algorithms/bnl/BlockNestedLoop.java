package com.github.dkanellis.skyspark.api.algorithms.bnl;

import org.apache.spark.api.java.JavaPairRDD;

import java.awt.geom.Point2D;
import java.util.List;

/**
 * @author Dimitris Kanellis
 */
public class BlockNestedLoop extends BlockNestedLoopTemplate {

    @Override
    protected JavaPairRDD<PointFlag, Point2D> sortRdd(JavaPairRDD<PointFlag, Point2D> flagPointPairs) {
        return flagPointPairs;
    }

    @Override
    protected void globalAddDiscardOrDominate(List<Point2D> globalSkylines, Point2D candidateGlobalSkylinePoint) {
        localAddDiscardOrDominate(globalSkylines, candidateGlobalSkylinePoint);
    }

    @Override
    public String toString() {
        return "BlockNestedLoop";
    }

}
