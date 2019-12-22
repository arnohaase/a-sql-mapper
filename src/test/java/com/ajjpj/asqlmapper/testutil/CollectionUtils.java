package com.ajjpj.asqlmapper.testutil;

import java.util.*;

/**
 * convenience code to help working with Java 8
 */
public class CollectionUtils {
    public static <T> Set<T> setOf(T... args) {
        final Set<T> result = new HashSet<>(Arrays.asList(args));
        return Collections.unmodifiableSet(result);
    }

    public static <T> List<T> listOf(T... args) {
        return Collections.unmodifiableList(Arrays.asList(args));
    }
}
