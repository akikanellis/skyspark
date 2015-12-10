package com.github.dkanellis.skyspark.api.utils.point;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.util.Comparator;

public class DominationComparator implements Comparator<Point2D>, Serializable {

    @Override
    public int compare(Point2D p, Point2D q) {
        double sumP = p.getX() + p.getY();
        double sumQ = q.getX() + q.getY();

        if (sumP < sumQ) {
            return -1;
        } else if (sumQ < sumP) {
            return 1;
        } else {
            return 0;
        }
    }
}
