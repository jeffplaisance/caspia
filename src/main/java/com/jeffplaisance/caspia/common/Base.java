package com.jeffplaisance.caspia.common;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

public final class Base {
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    public static int sum(List<Boolean> booleans) {
        return Collections.frequency(booleans, Boolean.TRUE);
    }

    public static int lessThanHalf(int n) {
        return (n-1)>>>1;
    }
}
