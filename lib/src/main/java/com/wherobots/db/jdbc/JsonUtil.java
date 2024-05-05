package com.wherobots.db.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private JsonUtil() {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String serialize(Object o) throws IllegalArgumentException {
        try {
            return MAPPER.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T> T deserialize(String json, TypeReference<T> type) throws IllegalArgumentException {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
