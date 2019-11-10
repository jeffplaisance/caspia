package com.jeffplaisance.caspia.common;

import com.google.common.base.Charsets;

public class NonDelimitedStringTranscoder implements Transcoder<String> {
    @Override
    public byte[] toBytes(String s) {
        return s.getBytes(Charsets.UTF_8);
    }

    @Override
    public String fromBytes(byte[] bytes) {
        return new String(bytes, Charsets.UTF_8);
    }
}
