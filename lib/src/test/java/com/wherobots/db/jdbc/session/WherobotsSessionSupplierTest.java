package com.wherobots.db.jdbc.session;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WherobotsSessionSupplierTest {

    @Test
    void buildSessionUriOmitsRegionWhenNull() {
        String uri = WherobotsSessionSupplier.buildSessionUri("api.example.com", null, false);
        assertEquals("https://api.example.com/sql/session?force_new=false", uri);
        assertFalse(uri.contains("region="));
    }

    @Test
    void buildSessionUriOmitsRegionWhenBlank() {
        String uri = WherobotsSessionSupplier.buildSessionUri("api.example.com", "   ", true);
        assertFalse(uri.contains("region="));
        assertTrue(uri.contains("force_new=true"));
    }

    @Test
    void buildSessionUriIncludesRegionWhenSet() {
        String uri = WherobotsSessionSupplier.buildSessionUri(
                "api.example.com", "byoc-acme-us-east-1", false);
        assertTrue(uri.contains("region=byoc-acme-us-east-1"));
    }
}
