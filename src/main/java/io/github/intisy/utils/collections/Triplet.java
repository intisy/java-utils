package io.github.intisy.utils.collections;

/**
 * A generic class representing a triplet of objects: a key and two associated values.
 * This class is used to store three related objects together, providing methods to
 * access and modify each component.
 *
 * @param <K> the type of the key
 * @param <V1> the type of the first value
 * @param <V2> the type of the second value
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class Triplet<K, V1, V2> {
    private K key;
    private V1 value1;
    private V2 value2;

    /**
     * Constructs a new Triplet with the specified key and two values.
     *
     * @param key the key of this triplet
     * @param value1 the first value of this triplet
     * @param value2 the second value of this triplet
     */
    public Triplet(K key, V1 value1, V2 value2) {
        this.key = key;
        this.value1 = value1;
        this.value2 = value2;
    }

    /**
     * Returns the key of this triplet.
     *
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * Sets the key of this triplet.
     *
     * @param key the new key to set
     */
    public void setKey(K key) {
        this.key = key;
    }

    /**
     * Returns the first value of this triplet.
     *
     * @return the first value
     */
    public V1 getValue1() {
        return value1;
    }

    /**
     * Sets the first value of this triplet.
     *
     * @param value1 the new first value to set
     */
    public void setValue1(V1 value1) {
        this.value1 = value1;
    }

    /**
     * Returns the second value of this triplet.
     *
     * @return the second value
     */
    public V2 getValue2() {
        return value2;
    }

    /**
     * Sets the second value of this triplet.
     *
     * @param value2 the new second value to set
     */
    public void setValue2(V2 value2) {
        this.value2 = value2;
    }

    /**
     * Returns a string representation of this triplet.
     * The string representation consists of the key and both values separated by commas
     * and enclosed in brackets.
     *
     * @return a string representation of this triplet
     */
    @Override
    public String toString() {
        return "Triplet{" +
                "key=" + key +
                ", value1=" + value1 +
                ", value2=" + value2 +
                '}';
    }
}
