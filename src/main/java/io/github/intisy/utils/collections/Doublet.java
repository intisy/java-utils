package io.github.intisy.utils.collections;

/**
 * A generic class representing a key-value pair.
 * This class is used to store two related objects together, typically as a key and its associated value.
 * It provides methods to access and modify both the key and value.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class Doublet<K, V> {
    private K key;
    private V value;

    /**
     * Constructs a new Doublet with the specified key and value.
     *
     * @param key the key of this doublet
     * @param value the value of this doublet
     */
    public Doublet(K key, V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the key of this doublet.
     *
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * Sets the key of this doublet.
     *
     * @param key the new key to set
     */
    public void setKey(K key) {
        this.key = key;
    }

    /**
     * Returns the value of this doublet.
     *
     * @return the value
     */
    public V getValue() {
        return value;
    }

    /**
     * Sets the value of this doublet.
     *
     * @param value1 the new value to set
     */
    public void setValue1(V value1) {
        this.value = value1;
    }

    /**
     * Returns a string representation of this doublet.
     * The string representation consists of the key and value separated by commas
     * and enclosed in brackets.
     *
     * @return a string representation of this doublet
     */
    @Override
    public String toString() {
        return "Triplet{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
