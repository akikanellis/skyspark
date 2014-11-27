package com.github.dkanellis.skyspark.api.math.point;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Dimitris Kanellis
 */
public class PointFlagTest {

    /**
     * Test of equals method, of class PointFlag.
     */
    @Test
    public void testEquals() {
        System.out.println("equals");
        PointFlag o = new PointFlag(1, 1);
        PointFlag instance = new PointFlag(1, 1);
        boolean result = instance.equals(o);
        assertTrue(result);
    }

    @Test
    public void testNotEquals() {
        System.out.println("not equals");
        PointFlag o = new PointFlag(1, 1);
        PointFlag instance = new PointFlag(0, 1);
        boolean result = instance.equals(o);
        assertFalse(result);
    }
}
