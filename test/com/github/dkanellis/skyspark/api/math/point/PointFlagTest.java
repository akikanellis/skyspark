package com.github.dkanellis.skyspark.api.math.point;

import com.github.dkanellis.skyspark.api.testcategories.SmallInputTest;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.experimental.categories.Category;

/**
 *
 * @author Dimitris Kanellis
 */
@Category(SmallInputTest.class)
public class PointFlagTest {

    /**
     * Test of equals method, of class PointFlag.
     */
    @Test
    public void shouldBeEquals() {
        System.out.println("equals");
        PointFlag o = new PointFlag(1, 1);
        PointFlag instance = new PointFlag(1, 1);
        boolean result = instance.equals(o);
        assertTrue(result);
    }

    @Test
    public void shouldNotBeEquals() {
        System.out.println("not equals");
        PointFlag o = new PointFlag(1, 1);
        PointFlag instance = new PointFlag(0, 1);
        boolean result = instance.equals(o);
        assertFalse(result);
    }
}
