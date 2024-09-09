package io.github.intisy.utils.custom;

@SuppressWarnings("unused")
public class Double<K, V> {
    private K key;
    private V value;

    public Double(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue1(V value1) {
        this.value = value1;
    }

    @Override
    public String toString() {
        return "Triplet{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
