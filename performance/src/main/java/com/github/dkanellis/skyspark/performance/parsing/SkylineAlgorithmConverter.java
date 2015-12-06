package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import com.github.dkanellis.skyspark.api.algorithms.Preconditions;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.Bitmap;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.BlockNestedLoop;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SortFilterSkyline;

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
