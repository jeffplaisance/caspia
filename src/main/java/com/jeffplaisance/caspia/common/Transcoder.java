package com.jeffplaisance.caspia.common;

public interface Transcoder<T> {
    byte[] toBytes(T t);

    T fromBytes(byte[] bytes);
}
