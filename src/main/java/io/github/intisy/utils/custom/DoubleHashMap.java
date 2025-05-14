package io.github.intisy.utils.custom;

import io.github.intisy.utils.custom.Doublet;
import io.github.intisy.utils.custom.Triplet;

import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;

public class DoubleHashMap<K, V1, V2> extends HashMap<K, Doublet<V1, V2>> {
    public void put(K key, V1 value1, V2 value2) {
        super.put(key, new Doublet<>(value1, value2));
    }

    public V1 getValue1(K key) {
        return get(key).getKey();
    }

    public V2 getValue2(K key) {
        return get(key).getValue();
    }


    public Collection<V1> values1() {
        return values().stream().map(Doublet::getKey).collect(Collectors.toList());
    }

    public Collection<V2> values2() {
        return values().stream().map(Doublet::getValue).collect(Collectors.toList());
    }

}

