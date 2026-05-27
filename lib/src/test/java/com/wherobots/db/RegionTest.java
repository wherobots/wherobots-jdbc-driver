package com.wherobots.db;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RegionTest {

    @Test
    void toApiValueMapsLegacyEnumName() {
        // Backward compatibility: the enum constant name maps to its API value.
        assertEquals("aws-us-west-2", Region.toApiValue("AWS_US_WEST_2"));
    }

    @Test
    void toApiValuePassesThroughApiValue() {
        assertEquals("aws-us-west-2", Region.toApiValue("aws-us-west-2"));
    }

    @Test
    void toApiValuePassesThroughByocRegion() {
        assertEquals("byoc-acme-us-east-1", Region.toApiValue("byoc-acme-us-east-1"));
    }

    @Test
    void toApiValueReturnsNullForNull() {
        assertNull(Region.toApiValue(null));
    }
}
