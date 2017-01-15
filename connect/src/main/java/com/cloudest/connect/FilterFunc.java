package com.cloudest.connect;

public interface FilterFunc<K, V> {
    boolean apply(K key, V value);
}
