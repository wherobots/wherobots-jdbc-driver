package com.wherobots.db;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Data compression codecs.
 * <p>
 * Note that those names are purposefully lowercase to match the SQL Session server's expectations.
 *
 * @author mpetazzoni
 */
public enum DataCompression {
    none(in -> in),
    lz4(in -> in),
    zstd(ZstdCompressorInputStream::new);

    // The Java Arrow library doesn't support other compression codecs at this time.

    public final Decompressor decompressor;

    DataCompression(Decompressor decompressor) {
        this.decompressor = decompressor;
    }

    @FunctionalInterface
    public interface Decompressor {
        InputStream get(InputStream in) throws IOException;
    }
}
