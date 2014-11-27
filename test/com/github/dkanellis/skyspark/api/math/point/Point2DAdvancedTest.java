package com.github.dkanellis.skyspark.api.math.point;

import java.awt.geom.Point2D;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Dimitris Kanellis
 */
public class Point2DAdvancedTest {

    /**
     * Test of dominates method, of class Point2DAdvanced.
     */
    @Test
    public void testDominatesDoublePoint() {
        System.out.println("dominates Point2D.Double");
        Point2D.Double otherDoublePoint = new Point2D.Double(5756.202069941658, 4418.941667115589);
        Point2DAdvanced instance = new Point2DAdvanced(2080.877494624074, 2302.0770958188664);
        boolean result = instance.dominates(otherDoublePoint);
        assertTrue(result);
    }
    
    @Test
    public void testNotDominatesDoublePoint() {
        System.out.println("doesn't dominate Point2D.Double");
        Point2D.Double otherDoublePoint = new Point2D.Double(6803.314583926934, 2266.355737840431);
        Point2DAdvanced instance = new Point2DAdvanced(5756.202069941658, 4418.941667115589);
        boolean result = instance.dominates(otherDoublePoint);
        assertFalse(result);
    }
    
    @Test
    public void testDominatesPoint2DAdvanced() {
        System.out.println("dominates Point2DAdvanced");
        Point2DAdvanced otherPoint = new Point2DAdvanced(9221.01165298033, 7140.932870341026);
        Point2DAdvanced instance = new Point2DAdvanced(6803.314583926934, 2266.355737840431);
        boolean result = instance.dominates(otherPoint);
        assertTrue(result);
    }
    
    @Test
    public void testNotDominatesPoint2DAdvanced() {
        System.out.println("doesn't dominate Point2DAdvanced");
        Point2D.Double otherDoublePoint = new Point2D.Double(9754.177023140166, 4285.318012311965);
        Point2DAdvanced instance = new Point2DAdvanced(9221.01165298033, 7140.932870341026);
        boolean result = instance.dominates(otherDoublePoint);
        assertFalse(result);
    }

    /**
     * Test of equals method, of class Point2DAdvanced.
     */
    @Test
    public void testEquals() {
        System.out.println("equals");
        Point2DAdvanced o = new Point2DAdvanced(9754.177023140166, 4285.318012311965);
        Point2DAdvanced instance = new Point2DAdvanced(9754.177023140166, 4285.318012311965);
        boolean result = instance.equals(o);
        assertTrue(result);
    }
    
    @Test
    public void testNotEquals() {
        System.out.println("not equals");
        Point2DAdvanced o = new Point2DAdvanced(9754.177023140166, 4285.318012311965);
        Point2DAdvanced instance = new Point2DAdvanced(1200.0795729848512, 6734.184911753793);
        boolean result = instance.equals(o);
        assertFalse(result);
    }

    /**
     * Test of fromTextLine method, of class Point2DAdvanced.
     */
    @Test
    public void testFromTextLine() {
        System.out.println("fromTextLine");
        String textLine = " 1200.0795729848512 6734.184911753793";
        String delimiter = " ";
        Point2DAdvanced expResult = new Point2DAdvanced(1200.0795729848512, 6734.184911753793);
        Point2DAdvanced result = Point2DAdvanced.fromTextLine(textLine, delimiter);
        assertEquals(expResult, result);
    }    
}
