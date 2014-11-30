package com.github.dkanellis.skyspark.api.algorithms.sparkimplementations;

import com.github.dkanellis.skyspark.api.algorithms.wrappers.SparkContextWrapper;
import com.github.dkanellis.skyspark.api.math.point.PointFlag;
import com.github.dkanellis.skyspark.api.testcategories.BasicTest;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.Tuple2;

/**
 *
 * @author Dimitris Kanellis
 */
@Category(BasicTest.class)
public class BlockNestedLoopTest {

    private static SparkContextWrapper sparkContext;

    public BlockNestedLoopTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        sparkContext = new SparkContextWrapper("BlockNestedLoopTest", "local");
    }

    @AfterClass
    public static void tearDownClass() {
        sparkContext.stop();
    }

    @Test
    public void shouldReturnTheSameRddUnsorted() {
        System.out.println("sortRDD");
        List<Tuple2<PointFlag, Point2D>> flagPointPairs = getFlagPointPairs();
        JavaPairRDD<PointFlag, Point2D> flagPointRdd = sparkContext.parallelizePairs(flagPointPairs);
        BlockNestedLoop instance = new BlockNestedLoop(sparkContext);

        JavaPairRDD<PointFlag, Point2D> expResult = flagPointRdd;
        JavaPairRDD<PointFlag, Point2D> result = instance.sortRDD(flagPointRdd);

        assertEquals(expResult.collect(), result.collect());
    }

    private List<Tuple2<PointFlag, Point2D>> getFlagPointPairs() {
        List<Tuple2<PointFlag, Point2D>> flagPointPairs = new ArrayList<>();
        flagPointPairs.add(new Tuple2(new PointFlag(0, 0), new Point2D.Double(2080.877494624074, 2302.0770958188664)));
        flagPointPairs.add(new Tuple2(new PointFlag(1, 0), new Point2D.Double(6803.314583926934, 2266.355737840431)));
        flagPointPairs.add(new Tuple2(new PointFlag(1, 1), new Point2D.Double(5756.202069941658, 4418.941667115589)));
        flagPointPairs.add(new Tuple2(new PointFlag(1, 1), new Point2D.Double(4610.505826490165, 3570.466435170513)));
        flagPointPairs.add(new Tuple2(new PointFlag(1, 1), new Point2D.Double(3475.4615053558455, 3488.0557122269856)));

        return flagPointPairs;
    }

    @Test
    public void shouldJustBeAddedInTheSkylineList() {
        System.out.println("globalAddDiscardOrDominate - added");
        List<Point2D> globalSkylines = getGlobalSkylines();
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(7594.778386003634, 2038.5448103420463);
        BlockNestedLoop instance = new BlockNestedLoop(sparkContext);

        List<Point2D> expResult = getGlobalSkylines();
        expResult.add(candidateGlobalSkylinePoint);
        instance.globalAddDiscardOrDominate(globalSkylines, candidateGlobalSkylinePoint);
        List<Point2D> result = globalSkylines;

        assertEquals(result, expResult);
    }

    @Test
    public void shouldNotBeAddedInTheSkylineList() {
        System.out.println("globalAddDiscardOrDominate - not added");
        List<Point2D> globalSkylines = getGlobalSkylines();
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(5300.723604353442, 6586.544252547646);
        BlockNestedLoop instance = new BlockNestedLoop(sparkContext);

        List<Point2D> expResult = getGlobalSkylines();
        instance.globalAddDiscardOrDominate(globalSkylines, candidateGlobalSkylinePoint);
        List<Point2D> result = globalSkylines;

        assertEquals(result, expResult);
    }

    @Test
    public void shouldBeAddedInTheSkylineListAndRemoveOneElement() {
        System.out.println("globalAddDiscardOrDominate - add and remove");
        List<Point2D> globalSkylines = getGlobalSkylines();
        Point2D pointToBeRemoved = new Point2D.Double(5413.326818883628, 4253.422322942394);
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(5400.723604353442, 4100.426522944594);
        BlockNestedLoop instance = new BlockNestedLoop(sparkContext);

        List<Point2D> expResult = getGlobalSkylines();
        expResult.remove(pointToBeRemoved);
        expResult.add(candidateGlobalSkylinePoint);
        instance.globalAddDiscardOrDominate(globalSkylines, candidateGlobalSkylinePoint);
        List<Point2D> result = globalSkylines;

        assertEquals(result, expResult);
    }
    
    @Test
    public void shouldBeAddedInTheSkylineListAndRemoveAll() {
        System.out.println("globalAddDiscardOrDominate - add and remove");
        List<Point2D> globalSkylines = getGlobalSkylines();
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(100.723604353442, 200.426522944594);
        BlockNestedLoop instance = new BlockNestedLoop(sparkContext);

        List<Point2D> expResult = new ArrayList<>();
        expResult.add(candidateGlobalSkylinePoint);
        instance.globalAddDiscardOrDominate(globalSkylines, candidateGlobalSkylinePoint);
        List<Point2D> result = globalSkylines;

        assertEquals(result, expResult);
    }

    private List<Point2D> getGlobalSkylines() {
        List<Point2D> points = new ArrayList<>();
        points.add(new Point2D.Double(5413.326818883628, 4253.422322942394));
        points.add(new Point2D.Double(6543.712935561507, 3075.282119298148));
        points.add(new Point2D.Double(5225.776905052095, 4418.640443578733));
        points.add(new Point2D.Double(7032.0677945754105, 2556.957955903529));
        points.add(new Point2D.Double(7816.246529527959, 1874.5766294965456));

        return points;
    }

}
