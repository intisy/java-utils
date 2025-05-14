package io.github.intisy.utils.utils;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class MapUtils {
    public static <K, V> LinkedHashMap<K, V> newLinkedHashMap(Object... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Must provide key-value pairs.");
        }
        LinkedHashMap<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            @SuppressWarnings("unchecked")
            K key = (K) keysAndValues[i];
            @SuppressWarnings("unchecked")
            V value = (V) keysAndValues[i + 1];
            map.put(key, value);
        }
        return map;
    }

    public static <K, V> HashMap<K, V> newHashMap(Object... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Must provide key-value pairs.");
        }
        HashMap<K, V> map = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            @SuppressWarnings("unchecked")
            K key = (K) keysAndValues[i];
            @SuppressWarnings("unchecked")
            V value = (V) keysAndValues[i + 1];
            map.put(key, value);
        }
        return map;
    }
}
