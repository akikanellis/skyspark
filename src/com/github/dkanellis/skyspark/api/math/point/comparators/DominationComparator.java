package com.github.dkanellis.skyspark.api.math.point.comparators;

import com.github.dkanellis.skyspark.api.math.point.Point2DAdvanced;
import java.io.Serializable;
import java.util.Comparator;

/**
 *
 * @author Dimitris Kanellis
 */
public class DominationComparator implements Comparator<Point2DAdvanced>, Serializable {

    @Override
    public int compare(Point2DAdvanced p, Point2DAdvanced q) {
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
