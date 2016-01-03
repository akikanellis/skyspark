package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.ParameterException;
import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.bitmap.Bitmap;
import com.github.dkanellis.skyspark.api.algorithms.bnl.BlockNestedLoop;
import com.github.dkanellis.skyspark.api.algorithms.sfs.SortFilterSkyline;
import com.github.dkanellis.skyspark.api.test_utils.categories.types.UnitTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

@Category(UnitTests.class)
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