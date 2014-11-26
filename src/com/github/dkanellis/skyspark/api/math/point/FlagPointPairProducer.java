package com.github.dkanellis.skyspark.api.math.point;

import java.io.Serializable;
import scala.Tuple2;

/**
 *
 * @author Dimitris Kanellis
 */
public class FlagPointPairProducer implements Serializable {

    private final Point2DAdvanced medianPoint;

    public FlagPointPairProducer(Point2DAdvanced medianPoint) {
        this.medianPoint = medianPoint;
    }

    public Tuple2<PointFlag, Point2DAdvanced> getFlagPointPair(Point2DAdvanced point) {
        PointFlag flag = calculateFlag(point);
        return new Tuple2<>(flag, point);
    }

    private PointFlag calculateFlag(Point2DAdvanced point) {
        double x = point.getX();
        double y = point.getY();
        double medianX = medianPoint.getX();
        double medianY = medianPoint.getY();

        int xBit = x < medianX ? 0 : 1;
        int yBit = y < medianY ? 0 : 1;

        return new PointFlag(xBit, yBit);
    }
}
