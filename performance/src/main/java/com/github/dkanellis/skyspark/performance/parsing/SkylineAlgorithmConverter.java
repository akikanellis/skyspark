package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import com.github.dkanellis.skyspark.api.algorithms.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.bnl.BlockNestedLoop;
import com.github.dkanellis.skyspark.api.algorithms.sfs.SortFilterSkyline;
import com.github.dkanellis.skyspark.api.utils.Preconditions;

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
                return null; // TODO new Bitmap();
            default:
                throw new ParameterException("Wrong algorithm value: " + value);
        }
    }
}
