/*
Copyright 2023 Jeff Plaisance

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

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
