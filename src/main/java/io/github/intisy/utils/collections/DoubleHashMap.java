package io.github.intisy.utils.collections;

import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * A specialized HashMap that stores a key mapped to two values using the Doublet class.
 * This class extends HashMap and provides additional methods to work with the two values
 * associated with each key.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V1> the type of the first value
 * @param <V2> the type of the second value
 * @author Finn Birich
 */

@SuppressWarnings("unused")
public class DoubleHashMap<K, V1, V2> extends HashMap<K, Doublet<V1, V2>> {
  /**
   * Associates the specified key with the specified values in this map.
   * If the map previously contained a mapping for the key, the old value is replaced.
   *
   * @param key the key with which the specified values are to be associated
   * @param value1 the first value to be associated with the specified key
   * @param value2 the second value to be associated with the specified key
   */
  public void put(K key, V1 value1, V2 value2) {
    super.put(key, new Doublet<>(value1, value2));
  }

  /**
   * Returns the first value to which the specified key is mapped,
   * or null if this map contains no mapping for the key.
   *
   * @param key the key whose associated first value is to be returned
   * @return the first value to which the specified key is mapped, or
   *         null if this map contains no mapping for the key
   * @throws NullPointerException if the mapping for the key is null
   */
  public V1 getValue1(K key) {
    return get(key).getKey();
  }

  /**
   * Returns the second value to which the specified key is mapped,
   * or null if this map contains no mapping for the key.
   *
   * @param key the key whose associated second value is to be returned
   * @return the second value to which the specified key is mapped, or
   *         null if this map contains no mapping for the key
   * @throws NullPointerException if the mapping for the key is null
   */
  public V2 getValue2(K key) {
    return get(key).getValue();
  }

  /**
   * Returns a collection view of the first values contained in this map.
   * The collection is backed by the map, so changes to the map are reflected in the collection.
   *
   * @return a collection view of the first values contained in this map
   */
  public Collection<V1> values1() {
    return values().stream().map(Doublet::getKey).collect(Collectors.toList());
  }

  /**
   * Returns a collection view of the second values contained in this map.
   * The collection is backed by the map, so changes to the map are reflected in the collection.
   *
   * @return a collection view of the second values contained in this map
   */
  public Collection<V2> values2() {
    return values().stream().map(Doublet::getValue).collect(Collectors.toList());
  }
}
