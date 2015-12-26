package com.github.dkanellis.skyspark.api.utils;

import java.util.List;

public class ListsTest {

    public static <T> boolean areEqualNoOrder(List<T> first, List<T> second) {
        return first.containsAll(second) && second.containsAll(first);
    }
}
