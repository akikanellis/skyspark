package com.github.dkanellis.skyspark.api.test_utils.data_mocks;

import com.github.dkanellis.skyspark.api.algorithms.bnl.PointFlag;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

public class FlagPointPairsMock {

    private static final List<Tuple2<PointFlag, Point2D>> FLAG_POINT_PAIRS_UNSORTED = new ArrayList<Tuple2<PointFlag, Point2D>>() {{
        add(new Tuple2<>(new PointFlag(0, 0), new Point2D.Double(2080.877494624074, 2302.0770958188664)));
        add(new Tuple2<>(new PointFlag(1, 0), new Point2D.Double(6803.314583926934, 2266.355737840431)));
        add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(5756.202069941658, 4418.941667115589)));
        add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(4610.505826490165, 3570.466435170513)));
        add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(3475.4615053558455, 3488.0557122269856)));
    }};

    public static List<Tuple2<PointFlag, Point2D>> getFlagPointPairsUnsorted() {
        return new ArrayList<>(FLAG_POINT_PAIRS_UNSORTED);
    }
}
