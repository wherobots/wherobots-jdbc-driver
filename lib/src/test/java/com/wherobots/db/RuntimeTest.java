package com.wherobots.db;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RuntimeTest {

    @Test
    void toApiValueMapsLegacyEnumName() {
        // Backward compatibility: the enum constant name maps to its API value.
        assertEquals("small", Runtime.toApiValue("SMALL"));
    }

    @Test
    void toApiValuePassesThroughApiValue() {
        assertEquals("x-large", Runtime.toApiValue("x-large"));
    }

    @Test
    void toApiValueReturnsNullForNull() {
        assertNull(Runtime.toApiValue(null));
    }
}
