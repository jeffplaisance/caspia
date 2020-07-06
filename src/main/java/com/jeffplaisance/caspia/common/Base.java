package com.jeffplaisance.caspia.common;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public final class Base {
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    public static int sum(List<Boolean> booleans) {
        return Collections.frequency(booleans, Boolean.TRUE);
    }

    public static int lessThanHalf(int n) {
        return (n-1)>>>1;
    }

    public static <T> Stream<T> toStream(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }
}
