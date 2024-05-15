package com.wherobots.db.jdbc.serde;

import com.wherobots.db.DataCompression;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ArrowUtil {
    private static final BufferAllocator ALLOCATOR = new RootAllocator();

    public static ArrowStreamReader readFrom(byte[] bytes, DataCompression compression) throws IOException {
        return new ArrowStreamReader(
                compression.decompressor.get(new ByteArrayInputStream(bytes)),
                ALLOCATOR,
                new CommonsCompressionFactory());

    }
}
