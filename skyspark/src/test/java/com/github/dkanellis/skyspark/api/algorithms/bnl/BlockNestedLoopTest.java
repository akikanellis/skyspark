package com.github.dkanellis.skyspark.api.algorithms.bnl;

import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import com.github.dkanellis.skyspark.api.test_utils.categories.types.SparkTests;
import com.github.dkanellis.skyspark.api.test_utils.dataMocks.FlagPointPairsMock;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(SparkTests.class)
public class BlockNestedLoopTest extends BaseSparkTest {

    private BlockNestedLoop blockNestedLoop;

    @Before
    public void setUp() {
        blockNestedLoop = new BlockNestedLoop();
    }

    @Test
    public void returnTheSameRddUnsorted() {
        JavaPairRDD<PointFlag, Point2D> expectedRdd = getSparkContextWrapper().parallelizePairs(FlagPointPairsMock.getFlagPointPairsUnsorted());

        JavaPairRDD<PointFlag, Point2D> actualRdd = blockNestedLoop.sortRdd(expectedRdd);

        assertEquals(expectedRdd.collect(), actualRdd.collect());
    }

    @Test
    public void isASkyline_willAddInTheSkylineList() {
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(7594.778386003634, 2038.5448103420463);
        List<Point2D> expectedSkylines = getGlobalSkylines();
        expectedSkylines.add(candidateGlobalSkylinePoint);

        List<Point2D> actualSkylines = getGlobalSkylines();
        blockNestedLoop.globalAddDiscardOrDominate(actualSkylines, candidateGlobalSkylinePoint);

        assertEquals(expectedSkylines, actualSkylines);
    }

    @Test
    public void isNotASkyline_wontAddInTheSkylineList() {
        List<Point2D> expectedSkylines = getGlobalSkylines();
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(5300.723604353442, 6586.544252547646);

        List<Point2D> actualSkylines = getGlobalSkylines();
        blockNestedLoop.globalAddDiscardOrDominate(actualSkylines, candidateGlobalSkylinePoint);

        assertEquals(expectedSkylines, actualSkylines);
    }

    @Test
    public void addInTheSkylineListAndRemoveOneElement() {
        Point2D pointToBeRemoved = new Point2D.Double(5413.326818883628, 4253.422322942394);
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(5400.723604353442, 4100.426522944594);
        List<Point2D> expectedSkylines = getGlobalSkylines();
        expectedSkylines.remove(pointToBeRemoved);
        expectedSkylines.add(candidateGlobalSkylinePoint);

        List<Point2D> actualSkylines = getGlobalSkylines();
        blockNestedLoop.globalAddDiscardOrDominate(actualSkylines, candidateGlobalSkylinePoint);

        assertEquals(expectedSkylines, actualSkylines);
    }

    @Test
    public void addInTheSkylineListAndRemoveAll() {
        Point2D candidateGlobalSkylinePoint = new Point2D.Double(100.723604353442, 200.426522944594);
        List<Point2D> expectedSkylines = new ArrayList<>();
        expectedSkylines.add(candidateGlobalSkylinePoint);

        List<Point2D> actualSkylines = getGlobalSkylines();
        blockNestedLoop.globalAddDiscardOrDominate(actualSkylines, candidateGlobalSkylinePoint);

        assertEquals(expectedSkylines, actualSkylines);
    }

    private List<Point2D> getGlobalSkylines() {
        List<Point2D> globalSkylines = new ArrayList<>();
        globalSkylines.add(new Point2D.Double(5413.326818883628, 4253.422322942394));
        globalSkylines.add(new Point2D.Double(6543.712935561507, 3075.282119298148));
        globalSkylines.add(new Point2D.Double(5225.776905052095, 4418.640443578733));
        globalSkylines.add(new Point2D.Double(7032.0677945754105, 2556.957955903529));
        globalSkylines.add(new Point2D.Double(7816.246529527959, 1874.5766294965456));

        return globalSkylines;
    }
}
