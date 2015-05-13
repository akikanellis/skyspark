package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.BlockNestedLoop;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SortFilterSkyline;
import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.PointFlag;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.Tuple2;
import suites.basic.BasicTestSuite;
import testcategories.BasicTest;

/**
 *
 * @author Dimitris Kanellis
 */
@Category(BasicTest.class)
public class SortFilterSkylineTest {

    private static SparkContextWrapper sparkContext;

    public SortFilterSkylineTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        if (BasicTestSuite.sparkContext == null) {
            sparkContext = new SparkContextWrapper("SortFilterSkylineTest", "local");
        } else {
            sparkContext = BasicTestSuite.sparkContext;
        }
    }

    @Test
    public void shouldReturnTheRddSorted() {
        System.out.println("sortRDD by coordinate totals");
        List<Tuple2<PointFlag, Point2D>> unsortedFlagPointPairs = getUnsortedFlagPointPairs();
        JavaPairRDD<PointFlag, Point2D> unsortedFlagPointRdd = sparkContext.parallelizePairs(unsortedFlagPointPairs);
        List<Tuple2<PointFlag, Point2D>> sortedFlagPointPairsByTotals = getSortedFlagPointPairsByTotals();
        SortFilterSkyline instance = new SortFilterSkyline(sparkContext);

        JavaPairRDD<PointFlag, Point2D> expResult = sparkContext.parallelizePairs(sortedFlagPointPairsByTotals);
        JavaPairRDD<PointFlag, Point2D> result = instance.sortRDD(unsortedFlagPointRdd);

        assertEquals(expResult.collect(), result.collect());
    }

    private List<Tuple2<PointFlag, Point2D>> getUnsortedFlagPointPairs() {
        List<Tuple2<PointFlag, Point2D>> flagPointPairs = new ArrayList<>();
        flagPointPairs.add(new Tuple2(new PointFlag(0, 0), new Point2D.Double(2080.877494624074, 2302.0770958188664)));
        flagPointPairs.add(new Tuple2(new PointFlag(1, 0), new Point2D.Double(6803.314583926934, 2266.355737840431)));
        flagPointPairs.add(new Tuple2(new PointFlag(1, 1), new Point2D.Double(5756.202069941658, 4418.941667115589)));
        flagPointPairs.add(new Tuple2(new PointFlag(1, 1), new Point2D.Double(4610.505826490165, 3570.466435170513)));
        flagPointPairs.add(new Tuple2(new PointFlag(1, 1), new Point2D.Double(3475.4615053558455, 3488.0557122269856)));

        return flagPointPairs;
    }

    private List<Tuple2<PointFlag, Point2D>> getSortedFlagPointPairsByTotals() {
        List<Tuple2<PointFlag, Point2D>> flagPointPairs = new ArrayList<>();
        flagPointPairs.add(
                new Tuple2(new PointFlag(0, 0), new Point2D.Double(2080.877494624074, 2302.0770958188664))); // 4382
        flagPointPairs.add(
                new Tuple2(new PointFlag(1, 1), new Point2D.Double(3475.4615053558455, 3488.0557122269856))); // 6963
        flagPointPairs.add(
                new Tuple2(new PointFlag(1, 1), new Point2D.Double(4610.505826490165, 3570.466435170513))); // 8180
        flagPointPairs.add(
                new Tuple2(new PointFlag(1, 0), new Point2D.Double(6803.314583926934, 2266.355737840431))); // 9069
        flagPointPairs.add(
                new Tuple2(new PointFlag(1, 1), new Point2D.Double(5756.202069941658, 4418.941667115589))); // 10174

        return flagPointPairs;
    }

    @Test
    public void shouldJustBeAddedInTheSkylineList() {
        System.out.println("globalAddDiscardOrDominate - added");
        List<Point2D> globalSkylines = getSortedGlobalSkylines();
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(7594.778386003634, 2200.5448103420463);
        SortFilterSkyline instance = new SortFilterSkyline(sparkContext);

        List<Point2D> expResult = getSortedGlobalSkylines();
        expResult.add(candidateGlobalSkylinePoint);
        instance.globalAddDiscardOrDominate(globalSkylines, candidateGlobalSkylinePoint);
        List<Point2D> result = globalSkylines;

        assertEquals(result, expResult);
    }

    @Test
    public void shouldNotBeAddedInTheSkylineList() {
        System.out.println("globalAddDiscardOrDominate - not added");
        List<Point2D> globalSkylines = getSortedGlobalSkylines();
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(5300.723604353442, 6586.544252547646);
        BlockNestedLoop instance = new BlockNestedLoop(sparkContext);

        List<Point2D> expResult = getSortedGlobalSkylines();
        instance.globalAddDiscardOrDominate(globalSkylines, candidateGlobalSkylinePoint);
        List<Point2D> result = globalSkylines;

        assertEquals(result, expResult);
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
