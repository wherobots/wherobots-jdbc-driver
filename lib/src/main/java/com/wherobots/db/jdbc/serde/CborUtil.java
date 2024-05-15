package com.wherobots.db.jdbc.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

public class CborUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper(new CBORFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static <T> T deserialize(byte[] bytes, TypeReference<T> type) throws IllegalArgumentException {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
