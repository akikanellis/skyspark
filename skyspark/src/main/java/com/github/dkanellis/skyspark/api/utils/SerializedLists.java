package com.github.dkanellis.skyspark.api.utils;

import com.clearspring.analytics.util.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public final class SerializedLists {

    private SerializedLists() {
        throw new AssertionError("No instances.");
    }

    public static <T> List<T> reverseAndKeepSerialized(Iterable<T> iterable) {
        List<T> list = Lists.newArrayList(iterable);
        return reverseAndKeepSerialized(list);
    }

    public static <T> List<T> reverseAndKeepSerialized(List<T> list) {
        List<T> reversed = new ArrayList<>();
        ListIterator<T> it = list.listIterator(list.size());

        while (it.hasPrevious()) {
            reversed.add(it.previous());
        }

        return reversed;
    }
}
