package com.github.dkanellis.skyspark.api.math.point.comparators;

import com.github.dkanellis.skyspark.api.math.point.Point2DAdvanced;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Dimitris Kanellis
 */
public class DominationComparatorTest {
    private DominationComparator comparator;
    
    @Before
    public void setUp() {
        this.comparator = new DominationComparator();
    }

    /**
     * Test of compare method, of class DominationComparator.
     */
    @Test
    public void testCompareSmaller() {
        System.out.println("compare smaller");
        Point2DAdvanced p = new Point2DAdvanced(502.345, 349.12);
        Point2DAdvanced q = new Point2DAdvanced(634.423, 542.11);
        int expResult = -1;
        int result = comparator.compare(p, q);
        assertEquals(expResult, result);
    }
    
    @Test
    public void testCompareLarger() {
        System.out.println("compare larger");
        Point2DAdvanced p = new Point2DAdvanced(634.423, 542.11);
        Point2DAdvanced q = new Point2DAdvanced(502.345, 349.12);
        int expResult = 1;
        int result = comparator.compare(p, q);
        assertEquals(expResult, result);
    }
    
    @Test
    public void testCompareEquals() {
        System.out.println("compare equals");
        Point2DAdvanced p = new Point2DAdvanced(634.423, 542.11);
        Point2DAdvanced q = new Point2DAdvanced(634.423, 542.11);
        int expResult = 0;
        int result = comparator.compare(p, q);
        assertEquals(expResult, result);
    }
    
    @Test
    public void testCompareEqualsReversedCoordinates() {
        System.out.println("compare equals reversed coordinates");
        Point2DAdvanced p = new Point2DAdvanced(634.423, 542.11);
        Point2DAdvanced q = new Point2DAdvanced(542.11, 634.423);
        int expResult = 0;
        int result = comparator.compare(p, q);
        assertEquals(expResult, result);
    }
}
