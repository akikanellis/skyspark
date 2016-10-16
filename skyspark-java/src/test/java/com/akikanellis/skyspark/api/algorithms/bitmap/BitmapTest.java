package com.akikanellis.skyspark.api.algorithms.bitmap;

import com.akikanellis.skyspark.api.test_utils.categories.types.SparkTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SparkTests.class)
public class BitmapTest {

    private Bitmap bitmap;

    @Before
    public void setUp() {
        bitmap = new Bitmap();
    }

    @Test
    public void toString_returnName() {
        String expectedName = "Bitmap";

        String actualName = bitmap.toString();

        assertEquals(expectedName, actualName);
    }
}