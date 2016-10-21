package com.akikanellis.skyspark.api.test_utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.mockito.mock.SerializableMode.ACROSS_CLASSLOADERS;

public class Mockitos {

    public static <T> T mockForSpark(Class<T> classToMock) {
        return mock(classToMock, withSettings().serializable(ACROSS_CLASSLOADERS));
    }
}
