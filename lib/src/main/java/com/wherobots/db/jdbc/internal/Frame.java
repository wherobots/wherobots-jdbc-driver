package com.wherobots.db.jdbc.internal;

import com.fasterxml.jackson.core.type.TypeReference;
import com.wherobots.db.jdbc.models.Event;
import com.wherobots.db.jdbc.serde.CborUtil;
import com.wherobots.db.jdbc.serde.JsonUtil;

import java.nio.ByteBuffer;

public record Frame(String s, ByteBuffer bytes, Exception error) {

    public Event get() throws Exception {
        if (s != null) {
            return JsonUtil.deserialize(s, new TypeReference<>() {});
        }

        if (bytes != null) {
            return CborUtil.deserialize(bytes.array(), new TypeReference<>() {});
        }

        throw error;
    }
}
