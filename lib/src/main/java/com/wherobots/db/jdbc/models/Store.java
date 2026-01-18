package com.wherobots.db.jdbc.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.wherobots.db.StorageFormat;

/**
 * Configuration for storing query results to cloud storage.
 * <p>
 * Examples:
 * <pre>{@code
 * // Store as a single file with a presigned URL for download (default parquet format)
 * Store.forDownload()
 *
 * // Store as a single CSV file with a presigned URL for download
 * Store.forDownload(StorageFormat.csv)
 * }</pre>
 *
 * @author mpetazzoni
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class Store {

    private final StorageFormat format;
    private final boolean single;
    private final boolean generatePresignedUrl;

    /**
     * Create a store configuration for downloading results via a presigned URL.
     * <p>
     * This is a convenience method that creates a configuration with single file mode
     * and presigned URL generation enabled, using the default format.
     *
     * @return a Store configured for download
     */
    public static Store forDownload() {
        return new Store(null, true, true);
    }

    /**
     * Create a store configuration for downloading results via a presigned URL.
     * <p>
     * This is a convenience method that creates a configuration with single file mode
     * and presigned URL generation enabled.
     *
     * @param format the storage format (parquet, csv, or geojson)
     * @return a Store configured for download
     */
    public static Store forDownload(StorageFormat format) {
        return new Store(format, true, true);
    }

    /**
     * Create a store configuration with all options.
     *
     * @param format the storage format (parquet, csv, or geojson)
     * @param single true to store as a single file, false for multiple files
     * @param generatePresignedUrl true to generate a presigned URL for the result
     * @throws IllegalArgumentException if generatePresignedUrl is true but single is false
     */
    private Store(StorageFormat format, boolean single, boolean generatePresignedUrl) {
        if (generatePresignedUrl && !single) {
            throw new IllegalArgumentException(
                    "Cannot generate a presigned URL without single file mode enabled");
        }
        this.format = format;
        this.single = single;
        this.generatePresignedUrl = generatePresignedUrl;
    }

    public StorageFormat getFormat() {
        return format;
    }

    public boolean isSingle() {
        return single;
    }

    public boolean isGeneratePresignedUrl() {
        return generatePresignedUrl;
    }
}
