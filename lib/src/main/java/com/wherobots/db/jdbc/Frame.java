package com.wherobots.db.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;

import java.nio.ByteBuffer;
import java.util.Map;

public record Frame(String s, ByteBuffer bytes, Exception error) {
    public Map<String, Object> get() throws Exception {
        if (s != null) {
            return JsonUtil.deserialize(s, new TypeReference<>() {});
        }

        if (bytes != null) {
            // TODO: CBOR
            return Map.of();
        }

        throw error;
    }
}
