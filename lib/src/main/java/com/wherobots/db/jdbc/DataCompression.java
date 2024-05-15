package com.wherobots.db.jdbc;

/**
 * Data compression codecs.
 * <p>
 * Note that those names are purposefully lowercase to match the SQL Session server's expectations.
 *
 * @author mpetazzoni
 */
public enum DataCompression {
    none,
    lz4,
    zstd,

    // The Java Arrow library doesn't support other compression codecs at this time.
}
