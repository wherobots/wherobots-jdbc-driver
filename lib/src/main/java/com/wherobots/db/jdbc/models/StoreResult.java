package com.wherobots.db.jdbc.models;

/**
 * Result information for a query that was stored to cloud storage.
 *
 * @param resultUri the URI or presigned URL of the stored result
 * @param size the size of the stored result in bytes, or null if not available
 * @author mpetazzoni
 */
public record StoreResult(String resultUri, Long size) {
}
