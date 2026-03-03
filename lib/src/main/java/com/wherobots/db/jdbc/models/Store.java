package com.wherobots.db.jdbc.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.wherobots.db.StorageFormat;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
 *
 * // Store as GeoJSON with additional storage options
 * Store.forDownload(StorageFormat.geojson, Map.of("ignoreNullFields", "false"))
 *
 * // Store as CSV with header option
 * Store.forDownload(StorageFormat.csv, Map.of("header", "true"))
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
    private final Map<String, String> options;

    /**
     * Create a store configuration for downloading results via a presigned URL.
     * <p>
     * This is a convenience method that creates a configuration with single file mode
     * and presigned URL generation enabled, using the default format.
     *
     * @return a Store configured for download
     */
    public static Store forDownload() {
        return new Store(null, true, true, null);
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
        return new Store(format, true, true, null);
    }

    /**
     * Create a store configuration for downloading results via a presigned URL,
     * with additional storage options.
     * <p>
     * This is a convenience method that creates a configuration with single file mode
     * and presigned URL generation enabled, with format-specific options. These options
     * correspond to the options available in Spark's {@code OPTIONS (...)} clause when
     * writing data.
     * <p>
     * Examples of options by format:
     * <ul>
     *   <li>GeoJSON: {@code ignoreNullFields} ({@code "true"}/{@code "false"})</li>
     *   <li>CSV: {@code header} ({@code "true"}/{@code "false"}), {@code delimiter}, {@code quote}</li>
     *   <li>Parquet: {@code compression} ({@code "snappy"}, {@code "gzip"}, etc.)</li>
     * </ul>
     *
     * @param format the storage format (parquet, csv, or geojson)
     * @param options additional format-specific storage options
     * @return a Store configured for download with options
     */
    public static Store forDownload(StorageFormat format, Map<String, String> options) {
        return new Store(format, true, true, options);
    }

    /**
     * Create a store configuration with all parameters.
     *
     * @param format the storage format (parquet, csv, or geojson)
     * @param single true to store as a single file, false for multiple files
     * @param generatePresignedUrl true to generate a presigned URL for the result
     * @param options additional format-specific storage options, or null for no options
     * @throws IllegalArgumentException if generatePresignedUrl is true but single is false
     */
    private Store(StorageFormat format, boolean single, boolean generatePresignedUrl, Map<String, String> options) {
        if (generatePresignedUrl && !single) {
            throw new IllegalArgumentException(
                    "Cannot generate a presigned URL without single file mode enabled");
        }
        this.format = format;
        this.single = single;
        this.generatePresignedUrl = generatePresignedUrl;
        this.options = options != null && !options.isEmpty()
                ? Collections.unmodifiableMap(new LinkedHashMap<>(options))
                : null;
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

    /**
     * Get the additional format-specific storage options.
     *
     * @return an unmodifiable map of options, or null if no options were specified
     */
    public Map<String, String> getOptions() {
        return options;
    }
}
