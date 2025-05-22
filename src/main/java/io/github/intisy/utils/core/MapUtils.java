package io.github.intisy.utils.core;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Utility class providing convenience methods for creating and manipulating maps.
 * This class includes methods for creating HashMap and LinkedHashMap instances
 * with initial key-value pairs in a concise syntax.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class MapUtils {
    /**
     * Creates a new LinkedHashMap with the specified key-value pairs.
     * This method provides a concise way to create and initialize a LinkedHashMap
     * in a single statement. The keys and values are provided as alternating arguments.
     *
     * @param keysAndValues alternating keys and values, must be an even number of arguments
     * @param <K> the type of keys maintained by the map
     * @param <V> the type of mapped values
     * @return a new LinkedHashMap containing the specified key-value pairs
     * @throws IllegalArgumentException if an odd number of arguments is provided
     * @throws ClassCastException if the keys or values cannot be cast to the expected types
     */
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

    /**
     * Creates a new HashMap with the specified key-value pairs.
     * This method provides a concise way to create and initialize a HashMap
     * in a single statement. The keys and values are provided as alternating arguments.
     *
     * @param keysAndValues alternating keys and values, must be an even number of arguments
     * @param <K> the type of keys maintained by the map
     * @param <V> the type of mapped values
     * @return a new HashMap containing the specified key-value pairs
     * @throws IllegalArgumentException if an odd number of arguments is provided
     * @throws ClassCastException if the keys or values cannot be cast to the expected types
     */
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
