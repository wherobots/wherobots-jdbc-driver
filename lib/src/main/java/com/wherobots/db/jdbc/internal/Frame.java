package com.wherobots.db.jdbc.internal;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public record Frame(String s, ByteBuffer bytes, Exception error) {

    private static final ObjectMapper MAPPER = new ObjectMapper(new CBORFactory());

    public Map<String, Object> get() throws Exception {
        if (s != null) {
            return JsonUtil.deserialize(s, new TypeReference<>() {});
        }

        if (bytes != null) {
            return MAPPER.readValue(bytes.array(), new TypeReference<>() {});
        }

        throw error;
    }
}
