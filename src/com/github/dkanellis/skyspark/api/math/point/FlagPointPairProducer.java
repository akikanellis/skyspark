package com.github.dkanellis.skyspark.api.math.point;

import scala.Tuple2;

import java.awt.geom.Point2D;
import java.io.Serializable;

/**
 * @author Dimitris Kanellis
 */
public class FlagPointPairProducer implements Serializable {

    private final Point2D medianPoint;

    public FlagPointPairProducer(Point2D medianPoint) {
        this.medianPoint = medianPoint;
    }

    public Tuple2<PointFlag, Point2D> getFlagPointPair(Point2D point) {
        PointFlag flag = calculateFlag(point);
        return new Tuple2<>(flag, point);
    }

    private PointFlag calculateFlag(Point2D point) {
        double x = point.getX();
        double y = point.getY();
        double medianX = medianPoint.getX();
        double medianY = medianPoint.getY();

        int xBit = x < medianX ? 0 : 1;
        int yBit = y < medianY ? 0 : 1;

        return new PointFlag(xBit, yBit);
    }
}
