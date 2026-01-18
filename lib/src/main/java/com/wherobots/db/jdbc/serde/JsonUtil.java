package com.wherobots.db.jdbc.serde;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonUtil {

    private JsonUtil() {}

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
            // SQL Session requires all values to be strings, and they get coerced back into the appropriate type.
            MAPPER.configOverride(boolean.class).setFormat(JsonFormat.Value.forShape(JsonFormat.Shape.STRING));
    }

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
