package com.akikanellis.skyspark.performance.parsing;

import com.akikanellis.skyspark.api.algorithms.OldSkylineAlgorithm;
import com.akikanellis.skyspark.api.algorithms.bitmap.Bitmap;
import com.akikanellis.skyspark.api.algorithms.bnl.OldBlockNestedLoop;
import com.akikanellis.skyspark.api.algorithms.sfs.SortFilterSkyline;
import com.beust.jcommander.ParameterException;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class SkylineAlgorithmConverterTest {

    SkylineAlgorithmConverter skylineAlgorithmConverter;

    @Before
    public void setUp() {
        skylineAlgorithmConverter = new SkylineAlgorithmConverter();
    }

    @Test
    public void bnl_returnBlockNestedLoop() {
        OldSkylineAlgorithm skylineAlgorithm = skylineAlgorithmConverter.convert(SkylineAlgorithmConverter.ALIAS_BLOCK_NESTED_LOOP);

        assertThat(skylineAlgorithm).isExactlyInstanceOf(OldBlockNestedLoop.class);
    }

    @Test
    public void sfs_returnSortFilterSkyline() {
        OldSkylineAlgorithm skylineAlgorithm = skylineAlgorithmConverter.convert(SkylineAlgorithmConverter.ALIAS_SORT_FILTER_SKYLINE);

        assertThat(skylineAlgorithm).isExactlyInstanceOf(SortFilterSkyline.class);
    }

    @Test
    public void bitmap_returnBitmap() {
        OldSkylineAlgorithm skylineAlgorithm = skylineAlgorithmConverter.convert(SkylineAlgorithmConverter.ALIAS_BITMAP);

        assertThat(skylineAlgorithm).isExactlyInstanceOf(Bitmap.class);
    }

    @Test
    public void unknown_throwException() {
        assertThatExceptionOfType(ParameterException.class)
                .isThrownBy(() -> skylineAlgorithmConverter.convert("random string"));
    }
}