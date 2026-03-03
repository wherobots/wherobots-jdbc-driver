package com.wherobots.db.jdbc.models;

import com.wherobots.db.StorageFormat;
import com.wherobots.db.jdbc.serde.JsonUtil;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StoreTest {

    @Test
    void forDownloadDefaultFormat() {
        Store store = Store.forDownload();
        assertNull(store.getFormat());
        assertTrue(store.isSingle());
        assertTrue(store.isGeneratePresignedUrl());
        assertNull(store.getOptions());

        String json = JsonUtil.serialize(store);
        assertFalse(json.contains("\"options\""), "options should be omitted when null");
        assertFalse(json.contains("\"format\""), "format should be omitted when null");
    }

    @Test
    void forDownloadWithFormat() {
        Store store = Store.forDownload(StorageFormat.csv);
        assertEquals(StorageFormat.csv, store.getFormat());
        assertTrue(store.isSingle());
        assertTrue(store.isGeneratePresignedUrl());
        assertNull(store.getOptions());

        String json = JsonUtil.serialize(store);
        assertTrue(json.contains("\"format\":\"csv\""));
        assertFalse(json.contains("\"options\""), "options should be omitted when null");
    }

    @Test
    void forDownloadWithFormatAndOptions() {
        Map<String, String> options = Map.of("ignoreNullFields", "false");
        Store store = Store.forDownload(StorageFormat.geojson, options);
        assertEquals(StorageFormat.geojson, store.getFormat());
        assertTrue(store.isSingle());
        assertTrue(store.isGeneratePresignedUrl());
        assertEquals(Map.of("ignoreNullFields", "false"), store.getOptions());

        String json = JsonUtil.serialize(store);
        assertTrue(json.contains("\"format\":\"geojson\""));
        assertTrue(json.contains("\"options\""));
        assertTrue(json.contains("\"ignoreNullFields\":\"false\""));
    }

    @Test
    void forDownloadWithMultipleOptions() {
        Map<String, String> options = Map.of(
                "header", "true",
                "delimiter", ","
        );
        Store store = Store.forDownload(StorageFormat.csv, options);

        String json = JsonUtil.serialize(store);
        assertTrue(json.contains("\"header\":\"true\""));
        assertTrue(json.contains("\"delimiter\":\",\""));
    }

    @Test
    void forDownloadWithEmptyOptions() {
        Store store = Store.forDownload(StorageFormat.csv, Map.of());
        assertNull(store.getOptions(), "empty options should be stored as null");

        String json = JsonUtil.serialize(store);
        assertFalse(json.contains("\"options\""), "empty options should be omitted from JSON");
    }

    @Test
    void forDownloadWithNullOptions() {
        Store store = Store.forDownload(StorageFormat.csv, null);
        assertNull(store.getOptions());

        String json = JsonUtil.serialize(store);
        assertFalse(json.contains("\"options\""), "null options should be omitted from JSON");
    }

    @Test
    void optionsAreImmutable() {
        Map<String, String> options = new java.util.HashMap<>();
        options.put("key", "value");
        Store store = Store.forDownload(StorageFormat.csv, options);

        // Modifying the original map should not affect the store
        options.put("extra", "value2");
        assertEquals(1, store.getOptions().size());

        // The returned map should be unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> {
            store.getOptions().put("extra", "value2");
        });
    }

    @Test
    void executeSqlRequestWithStoreOptions() {
        Map<String, String> options = Map.of("ignoreNullFields", "false");
        Store store = Store.forDownload(StorageFormat.geojson, options);
        ExecuteSqlRequest request = new ExecuteSqlRequest("exec-123", "SELECT 1", store);

        String json = JsonUtil.serialize(request);
        assertTrue(json.contains("\"execution_id\":\"exec-123\""));
        assertTrue(json.contains("\"statement\":\"SELECT 1\""));
        assertTrue(json.contains("\"store\""));
        assertTrue(json.contains("\"options\""));
        assertTrue(json.contains("\"ignoreNullFields\":\"false\""));
    }
}
