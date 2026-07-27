package com.wherobots.db.jdbc;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WherobotsJdbcDriverTest {

    // These tests overwrite JVM-wide system properties, so they restore them
    // afterwards rather than leaving them set for whatever runs next. Nothing
    // depends on that today only because every test that reads them sets them
    // first; test ordering or parallel execution would end that.
    private final Map<String, String> originalProperties = Map.of(
            "os.name", System.getProperty("os.name", ""),
            "java.version", System.getProperty("java.version", ""));

    @AfterEach
    void restoreSystemProperties() {
        originalProperties.forEach((key, value) -> {
            if (value.isEmpty()) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        });
    }

    @Test
    void getUserAgentHeader() {
        System.setProperty("java.version", "1");
        System.setProperty("os.name", "os1");
        WherobotsJdbcDriver driver = new WherobotsJdbcDriver();
        Map<String, String> header = driver.getUserAgentHeader();
        assert header.containsKey("User-Agent");
        String user_agent = header.get("User-Agent");
        assert user_agent.equals("wherobots-jdbc-driver/unknown os/os1 java/1");
    }

    @Test
    void getClientChainHeaderIsEmptyWithoutTheProperty() {
        WherobotsJdbcDriver driver = new WherobotsJdbcDriver();
        assertTrue(driver.getClientChainHeader(new Properties()).isEmpty());

        Properties blank = new Properties();
        blank.put(WherobotsJdbcDriver.CLIENT_CHAIN_PROP, "  ");
        assertTrue(driver.getClientChainHeader(blank).isEmpty());
    }

    @Test
    void getClientChainHeaderCarriesTheUpstreamChain() {
        Properties info = new Properties();
        info.put(WherobotsJdbcDriver.CLIENT_CHAIN_PROP, "client=cli;ver=1.2.0");

        Map<String, String> header = new WherobotsJdbcDriver().getClientChainHeader(info);
        assertEquals("client=cli;ver=1.2.0", header.get(ClientHeader.HEADER_NAME));
    }

    @Test
    void clientChainPropertyEndsUpLeftOfTheDriverHop() {
        System.setProperty("os.name", "Linux");
        Properties info = new Properties();
        info.put(WherobotsJdbcDriver.CLIENT_CHAIN_PROP, "client=claude_web, client=mcp;ver=0.9");

        // The value the session request will actually carry: the driver stages
        // the caller's chain, the session supplier appends the driver's hop.
        Map<String, String> headers = ClientHeader.withHop(
                new WherobotsJdbcDriver().getClientChainHeader(info));

        assertEquals("client=claude_web, client=mcp;ver=0.9, client=jdbc;ver="
                        + ClientHeader.UNKNOWN_VERSION + ";plat=linux",
                headers.get(ClientHeader.HEADER_NAME));
    }

    @Test
    void clientChainPropertyCannotInjectAnotherHeader() {
        Properties info = new Properties();
        info.put(WherobotsJdbcDriver.CLIENT_CHAIN_PROP, "client=cli\r\nAuthorization: Bearer evil");

        Map<String, String> headers = ClientHeader.withHop(
                new WherobotsJdbcDriver().getClientChainHeader(info));

        assertEquals(1, headers.size());
        String value = headers.get(ClientHeader.HEADER_NAME);
        assertFalse(value.contains("\r"), value);
        assertFalse(value.contains("\n"), value);
    }
}
