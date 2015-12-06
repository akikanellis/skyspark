package com.github.dkanellis.skyspark.performance.parsing;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import com.github.dkanellis.skyspark.api.algorithms.Preconditions;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.Bitmap;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.BlockNestedLoop;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SkylineAlgorithm;
import com.github.dkanellis.skyspark.api.algorithms.sparkimplementations.SortFilterSkyline;

public class SkylineAlgorithmConverter implements IStringConverter<SkylineAlgorithm> {
    @Override
    public SkylineAlgorithm convert(String value) {
        Preconditions.checkNotEmpty(value);
        value = value.toLowerCase();

        switch (value) {
            case "bnl":
                return new BlockNestedLoop();
            case "sfs":
                return new SortFilterSkyline();
            case "bitmap":
                return new Bitmap();
            default:
                throw new ParameterException("Wrong algorithm value: " + value);
        }
    }
}
