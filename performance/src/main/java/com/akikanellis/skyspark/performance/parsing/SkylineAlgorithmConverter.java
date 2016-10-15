package com.akikanellis.skyspark.performance.parsing;

import com.akikanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.akikanellis.skyspark.api.algorithms.bitmap.Bitmap;
import com.akikanellis.skyspark.api.algorithms.bnl.BlockNestedLoop;
import com.akikanellis.skyspark.api.algorithms.sfs.SortFilterSkyline;
import com.akikanellis.skyspark.api.utils.Preconditions;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public class SkylineAlgorithmConverter implements IStringConverter<SkylineAlgorithm> {

    static final String ALIAS_BLOCK_NESTED_LOOP = "bnl";
    static final String ALIAS_SORT_FILTER_SKYLINE = "sfs";
    static final String ALIAS_BITMAP = "bitmap";

    @Override
    public SkylineAlgorithm convert(String value) {
        Preconditions.checkNotEmpty(value);
        value = value.toLowerCase();

        switch (value) {
            case ALIAS_BLOCK_NESTED_LOOP:
                return new BlockNestedLoop();
            case ALIAS_SORT_FILTER_SKYLINE:
                return new SortFilterSkyline();
            case ALIAS_BITMAP:
                return new Bitmap();
            default:
                throw new ParameterException("Wrong algorithm value: " + value);
        }
    }
}
