package com.github.dkanellis.skyspark.api.algorithms.bitmap;

import com.github.dkanellis.skyspark.api.test_utils.base.BaseSparkTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BitmapTest extends BaseSparkTest {

    private Bitmap bitmap;

    @Before
    public void setUp() {
        bitmap = new Bitmap(getSparkContextWrapper(), 1);
    }


    @Test
    @Ignore("To be implemented")
    public void forEachXofPoint_returnBitVector() {

    }

    @Test
    @Ignore("To be implemented")
    public void forEachYofPoint_returnBitVector() {
    }

    @Test
    public void toString_returnName() {
        String expectedName = "Bitmap";

        String actualName = bitmap.toString();

        assertEquals(expectedName, actualName);
    }
}