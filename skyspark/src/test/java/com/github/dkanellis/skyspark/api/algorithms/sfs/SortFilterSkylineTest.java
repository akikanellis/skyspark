package com.github.dkanellis.skyspark.api.algorithms.sfs;

import com.github.dkanellis.skyspark.api.algorithms.bnl.PointFlag;
import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import com.github.dkanellis.skyspark.api.test_utils.categories.types.SparkTests;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.Tuple2;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(SparkTests.class)
public class SortFilterSkylineTest extends BaseSparkTest {

    private SortFilterSkyline sortFilterSkyline;

    @Before
    public void setUp() {
        sortFilterSkyline = new SortFilterSkyline();
    }

    @Test
    public void returnTheRddSorted() {
        JavaPairRDD<PointFlag, Point2D> expectedRdd
                = getSparkContextWrapper().parallelizePairs(getSortedFlagPointPairsByTotals());

        JavaPairRDD<PointFlag, Point2D> unsortedFlagPointRdd
                = getSparkContextWrapper().parallelizePairs(getUnsortedFlagPointPairs());
        JavaPairRDD<PointFlag, Point2D> actualRdd = sortFilterSkyline.sortRdd(unsortedFlagPointRdd);

        assertEquals(expectedRdd.collect(), actualRdd.collect());
    }

    private List<Tuple2<PointFlag, Point2D>> getUnsortedFlagPointPairs() {
        List<Tuple2<PointFlag, Point2D>> flagPointPairs = new ArrayList<>();
        flagPointPairs.add(new Tuple2<>(new PointFlag(0, 0), new Point2D.Double(2080.877494624074, 2302.0770958188664)));
        flagPointPairs.add(new Tuple2<>(new PointFlag(1, 0), new Point2D.Double(6803.314583926934, 2266.355737840431)));
        flagPointPairs.add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(5756.202069941658, 4418.941667115589)));
        flagPointPairs.add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(4610.505826490165, 3570.466435170513)));
        flagPointPairs.add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(3475.4615053558455, 3488.0557122269856)));

        return flagPointPairs;
    }

    private List<Tuple2<PointFlag, Point2D>> getSortedFlagPointPairsByTotals() {
        List<Tuple2<PointFlag, Point2D>> flagPointPairs = new ArrayList<>();
        flagPointPairs.add(new Tuple2<>(new PointFlag(0, 0), new Point2D.Double(2080.877494624074, 2302.0770958188664))); // 4382
        flagPointPairs.add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(3475.4615053558455, 3488.0557122269856))); // 6963
        flagPointPairs.add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(4610.505826490165, 3570.466435170513))); // 8180
        flagPointPairs.add(new Tuple2<>(new PointFlag(1, 0), new Point2D.Double(6803.314583926934, 2266.355737840431))); // 9069
        flagPointPairs.add(new Tuple2<>(new PointFlag(1, 1), new Point2D.Double(5756.202069941658, 4418.941667115589))); // 10174

        return flagPointPairs;
    }

    @Test
    public void isSkyline_addInTheSkylineList() {
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(7594.778386003634, 2200.5448103420463);
        List<Point2D> expectedSkylines = getSortedGlobalSkylines();
        expectedSkylines.add(candidateGlobalSkylinePoint);

        List<Point2D> actualSkylines = getSortedGlobalSkylines();
        sortFilterSkyline.globalAddDiscardOrDominate(actualSkylines, candidateGlobalSkylinePoint);

        assertEquals(expectedSkylines, actualSkylines);
    }

    @Test
    public void notASkyline_wontAddInTheSkylineList() {
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(5300.723604353442, 6586.544252547646);
        List<Point2D> expectedSkylines = getSortedGlobalSkylines();

        List<Point2D> globalSkylines = getSortedGlobalSkylines();
        sortFilterSkyline.globalAddDiscardOrDominate(globalSkylines, candidateGlobalSkylinePoint);

        assertEquals(expectedSkylines, globalSkylines);
    }

    private List<Point2D> getSortedGlobalSkylines() {
        List<Point2D> points = new ArrayList<>();
        points.add(new Point2D.Double(2080.877494624074, 2302.0770958188664)); // 4382
        points.add(new Point2D.Double(3475.4615053558455, 3488.0557122269856)); // 6963
        points.add(new Point2D.Double(4610.505826490165, 3570.466435170513)); // 8180
        points.add(new Point2D.Double(6803.314583926934, 2266.355737840431)); // 9069
        points.add(new Point2D.Double(5756.202069941658, 4418.941667115589)); // 10174

        return points;
    }
}
