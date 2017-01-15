package com.cloudest.mq.serde;

import java.util.Objects;

/**
 * @param <K> Key type
 * @param <V> Value type
 */
public class KeyValue<K, V> {

    private final K key;
    private final V value;

    public KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K, V> KeyValue<K, V> pair(K key, V value) {
        return new KeyValue<>(key, value);
    }

    public final K key() {
        return key;
    }

    public final V value() {
        return value;
    }

    public String toString() {
        return "KeyValue(" + key + ", " + value + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof KeyValue)) {
            return false;
        }

        KeyValue other = (KeyValue) obj;
        return (this.key == null ? other.key == null : this.key.equals(other.key))
                && (this.value == null ? other.value == null : this.value.equals(other.value));
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
