package com.github.dkanellis.skyspark.api.math.point.comparators;

import com.github.dkanellis.skyspark.api.testcategories.SmallInputTest;
import java.awt.geom.Point2D;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.experimental.categories.Category;

/**
 *
 * @author Dimitris Kanellis
 */
@Category(SmallInputTest.class)
public class DominationComparatorTest {
    /**
     * Test of compare method, of class DominationComparator.
     */
    @Test
    public void firstShouldBeSmallerThanSecond() {
        System.out.println("compare smaller");
        Point2D p = new Point2D.Double(502.345, 349.12);
        Point2D q = new Point2D.Double(634.423, 542.11);
        DominationComparator comparator = new DominationComparator();
        
        int expResult = -1;
        int result = comparator.compare(p, q);
        assertEquals(expResult, result);
    }
    
    @Test
    public void firstShouldBeLargerThanSecond() {
        System.out.println("compare larger");
        Point2D p = new Point2D.Double(634.423, 542.11);
        Point2D q = new Point2D.Double(502.345, 349.12);
        DominationComparator comparator = new DominationComparator();
        
        int expResult = 1;
        int result = comparator.compare(p, q);
        assertEquals(expResult, result);
    }
    
    @Test
    public void firstShouldBeEqualWithSecond() {
        System.out.println("compare equals");
        Point2D p = new Point2D.Double(634.423, 542.11);
        Point2D q = new Point2D.Double(634.423, 542.11);
        DominationComparator comparator = new DominationComparator();
        
        int expResult = 0;
        int result = comparator.compare(p, q);
        assertEquals(expResult, result);
    }
    
    @Test
    public void secondShouldBeEqualWithFirst() {
        System.out.println("compare equals reversed coordinates");
        Point2D p = new Point2D.Double(634.423, 542.11);
        Point2D q = new Point2D.Double(542.11, 634.423);
        DominationComparator comparator = new DominationComparator();
        
        int expResult = 0;
        int result = comparator.compare(p, q);
        assertEquals(expResult, result);
    }
}
