package com.jeffplaisance.caspia.common;

public interface ThrowingFunction<T,R,E extends Throwable> {
    R apply(T t) throws E;
}
