package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.ParameterException;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.Bitmap;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.BlockNestedLoop;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SortFilterSkyline;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class SkylineAlgorithmConverterTest {

    SkylineAlgorithmConverter skylineAlgorithmConverter;

    @Before
    public void setUp() {
        skylineAlgorithmConverter = new SkylineAlgorithmConverter();
    }

    @Test
    public void bnl_returnBlockNestedLoop() {
        SkylineAlgorithm skylineAlgorithm = skylineAlgorithmConverter.convert(SkylineAlgorithmConverter.ALIAS_BLOCK_NESTED_LOOP);

        assertThat(skylineAlgorithm, instanceOf(BlockNestedLoop.class));
    }

    @Test
    public void sfs_returnSortFilterSkyline() {
        SkylineAlgorithm skylineAlgorithm = skylineAlgorithmConverter.convert(SkylineAlgorithmConverter.ALIAS_SORT_FILTER_SKYLINE);

        assertThat(skylineAlgorithm, instanceOf(SortFilterSkyline.class));
    }

    @Test
    public void bitmap_returnBitmap() {
        SkylineAlgorithm skylineAlgorithm = skylineAlgorithmConverter.convert(SkylineAlgorithmConverter.ALIAS_BITMAP);

        assertThat(skylineAlgorithm, instanceOf(Bitmap.class));
    }

    @Test(expected = ParameterException.class)
    public void unknown_throwException() {
        skylineAlgorithmConverter.convert("random string");
    }
}