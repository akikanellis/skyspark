package com.github.dkanellis.skyspark.api.algorithms.bitmap;

public class Injector {
    public static BitmapStructure getBitmapStructure(final int numberOfPartitions) {
        BitSliceCreator bitSliceCreator = getBitSliceCreator();
        return new BitmapStructureImpl(numberOfPartitions, bitSliceCreator);
    }

    private static BitSliceCreator getBitSliceCreator() {
        return new BitSliceCreatorImpl();
    }
}
