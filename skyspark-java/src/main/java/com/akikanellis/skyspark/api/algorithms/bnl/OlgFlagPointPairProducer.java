package com.akikanellis.skyspark.api.algorithms.bnl;

import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.awt.geom.Point2D;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class holds the median point of a dataset and for every point given it outputs where that point should be in
 * the dataset, meaning a pair of <OldPointFlag, Point>.
 */
public class OlgFlagPointPairProducer implements Serializable {

    private final Point2D medianPoint;

    public OlgFlagPointPairProducer(@NotNull Point2D medianPoint) {
        this.medianPoint = checkNotNull(medianPoint);
    }

    public Tuple2<OldPointFlag, Point2D> getFlagPointPair(Point2D point) {
        OldPointFlag flag = calculateFlag(point);
        return new Tuple2<>(flag, point);
    }

    private OldPointFlag calculateFlag(Point2D point) {
        double x = point.getX();
        double y = point.getY();
        double medianX = medianPoint.getX();
        double medianY = medianPoint.getY();

        int xBit = x < medianX ? 0 : 1;
        int yBit = y < medianY ? 0 : 1;

        return new OldPointFlag(xBit, yBit);
    }
}
