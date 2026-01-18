package com.wherobots.db;

/**
 * Storage formats for storing query results to cloud storage.
 * <p>
 * Note that those names are purposefully lowercase to match the SQL Session server's expectations.
 *
 * @author mpetazzoni
 */
public enum StorageFormat {
    parquet,
    csv,
    geojson,
}
