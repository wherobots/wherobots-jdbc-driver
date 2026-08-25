package com.wherobots.db.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientHeaderTest {

    private final String originalOsName = System.getProperty("os.name");

    @AfterEach
    void restoreOsName() {
        if (originalOsName == null) {
            System.clearProperty("os.name");
        } else {
            System.setProperty("os.name", originalOsName);
        }
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    @Test
    void hopRendersTokenVersionAndPlatform() {
        assertEquals("client=jdbc;ver=0.4.0;plat=mac-os-x",
                ClientHeader.hop("0.4.0", "Mac OS X"));
    }

    @Test
    void hopDegradesToUnknownVersionWhenUnavailable() {
        // No JAR manifest (IDE / test / shaded-into-an-uber-jar runs) means no
        // Implementation-Version: the hop is still emitted and well-formed.
        assertEquals("client=jdbc;ver=unknown;plat=linux", ClientHeader.hop(null, "Linux"));
        assertEquals("client=jdbc;ver=unknown;plat=linux", ClientHeader.hop("   ", "Linux"));
    }

    @Test
    void hopOmitsPlatformWhenUnavailable() {
        assertEquals("client=jdbc;ver=0.4.0", ClientHeader.hop("0.4.0", null));
        assertEquals("client=jdbc;ver=0.4.0", ClientHeader.hop("0.4.0", ""));
    }

    @Test
    void hopSanitizesGrammarDelimitersOutOfParameters() {
        String hop = ClientHeader.hop("1.0;cmd=evil,client=spoofed", "li;nux");
        assertEquals("client=jdbc;ver=1.0-cmd-evil-client-spoofed;plat=li-nux", hop);
        // Exactly one hop, with only the delimiters we rendered ourselves.
        assertFalse(hop.contains(","));
        assertEquals(2, hop.chars().filter(c -> c == ';').count());
    }

    @Test
    void hopBoundsParameterLength() {
        String hop = ClientHeader.hop("v".repeat(200), "p".repeat(200));
        for (String param : hop.split(";")) {
            assertTrue(param.split("=", 2)[1].length() < 64,
                    "token must stay under 64 characters: " + param);
        }
    }

    @Test
    void hopTruncationDoesNotExposeATrailingSeparator() {
        // The 63-character cut lands right after the `,` that sanitizing
        // turned into a `-`, so stripping separators before truncating is not
        // enough -- the truncation itself can create a new trailing one.
        String version = "v".repeat(62) + ",rc1";

        String hop = ClientHeader.hop(version, "linux");

        assertEquals("client=jdbc;ver=" + "v".repeat(62) + ";plat=linux", hop);
    }

    @Test
    void valueWithoutUpstreamChainIsASingleHop() {
        System.setProperty("os.name", "Linux");
        String value = ClientHeader.value(null);
        assertFalse(value.contains(","), "an origin request emits exactly one hop");
        assertTrue(value.startsWith("client=jdbc;ver="), value);
        assertTrue(value.endsWith(";plat=linux"), value);
    }

    @Test
    void valueUsesTheManifestVersionWhenAvailable() {
        // Tests run from a class directory, so there is no manifest to read and
        // the graceful-degradation path is the one exercised end to end.
        String expectedVersion = ClientHeader.driverVersion();
        String expected = expectedVersion == null || expectedVersion.isBlank()
                ? ClientHeader.UNKNOWN_VERSION
                : expectedVersion;
        assertTrue(ClientHeader.value(null).contains(";ver=" + expected),
                ClientHeader.value(null));
    }

    @Test
    void valueAppendsOwnHopToTheRightOfAnUpstreamChain() {
        System.setProperty("os.name", "Linux");
        String value = ClientHeader.value("client=claude-web, client=mcp;ver=0.9");
        assertEquals("client=claude-web, client=mcp;ver=0.9, "
                + ClientHeader.hop(ClientHeader.driverVersion(), "Linux"), value);
        // Leftmost hop stays the origin; ours is the rightmost, direct caller.
        assertTrue(value.startsWith("client=claude-web,"), value);
        assertTrue(value.endsWith("plat=linux"), value);
    }

    @Test
    void valueNormalizesUpstreamChainSpacingAndEmptyHops() {
        System.setProperty("os.name", "Linux");
        String value = ClientHeader.value("  client=cli;ver=1.2.0 ,, ,  client=mcp  ");
        assertTrue(value.startsWith("client=cli;ver=1.2.0, client=mcp, client=jdbc;"), value);
    }

    @Test
    void valueStripsCharactersThatWouldInjectAHeaderOrCorruptTheGrammar() {
        String value = ClientHeader.value("client=cli\r\nX-Evil: 1\ttab\"quote\"");
        assertFalse(value.contains("\r"), value);
        assertFalse(value.contains("\n"), value);
        assertFalse(value.contains("\t"), value);
        assertFalse(value.contains("\""), value);
        assertTrue(value.startsWith("client=cli_"), value);
    }

    @Test
    void valueDropsAnUpstreamChainThatWouldBreakTheSizeBound() {
        System.setProperty("os.name", "Linux");
        String oversized = ("client=" + "x".repeat(40) + ", ").repeat(20);
        assertTrue(utf8Length(oversized) > ClientHeader.MAX_HEADER_BYTES);

        String value = ClientHeader.value(oversized);
        assertEquals(ClientHeader.hop(ClientHeader.driverVersion(), "Linux"), value);
        assertTrue(utf8Length(value) <= ClientHeader.MAX_HEADER_BYTES);
    }

    @Test
    void valueSanitizesNonAsciiBeforeMeasuringTheBound() {
        // Sanitization runs first, so the emitted value is always ASCII and its
        // byte length is what the bound is measured against.
        String value = ClientHeader.value("client=" + "é".repeat(600));
        assertTrue(utf8Length(value) <= ClientHeader.MAX_HEADER_BYTES);
        assertEquals(value.length(), utf8Length(value));
        assertTrue(value.startsWith("client=jdbc;"), value);
    }

    @Test
    void withHopSetsTheHeaderAndPreservesOtherHeaders() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-API-Key", "secret");
        headers.put("User-Agent", "wherobots-jdbc-driver/0.4.0");

        Map<String, String> result = ClientHeader.withHop(headers);

        assertEquals("secret", result.get("X-API-Key"));
        assertEquals("wherobots-jdbc-driver/0.4.0", result.get("User-Agent"));
        assertTrue(result.get(ClientHeader.HEADER_NAME).startsWith("client=jdbc;ver="));
        // The caller's map is left untouched.
        assertFalse(headers.containsKey(ClientHeader.HEADER_NAME));
    }

    @Test
    void withHopAppendsToADifferentlyCasedExistingHeader() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("x-wherobots-client", "client=cli;ver=1.2.0");

        Map<String, String> result = ClientHeader.withHop(headers);

        // Exactly one attribution header, under the canonical name.
        assertEquals(1, result.keySet().stream()
                .filter(key -> key.equalsIgnoreCase(ClientHeader.HEADER_NAME))
                .count());
        assertTrue(result.get(ClientHeader.HEADER_NAME)
                .startsWith("client=cli;ver=1.2.0, client=jdbc;"),
                result.get(ClientHeader.HEADER_NAME));
    }

    @Test
    void withHopKeepsEveryCaseVariantSpellingOfTheHeader() {
        // A Map<String, String> permits both spellings even though HTTP does
        // not. No caller produces this today, but collapsing to the last one
        // seen would silently drop `client=a`.
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("x-wherobots-client", "client=a");
        headers.put("X-Wherobots-Client", "client=b");

        Map<String, String> result = ClientHeader.withHop(headers);

        assertEquals(1, result.keySet().stream()
                .filter(key -> key.equalsIgnoreCase(ClientHeader.HEADER_NAME))
                .count());
        assertTrue(result.get(ClientHeader.HEADER_NAME)
                .startsWith("client=a, client=b, client=jdbc;"),
                result.get(ClientHeader.HEADER_NAME));
    }

    @Test
    void headersAreAcceptedByTheHttpRequestBuilder() {
        // java.net.http rejects illegal header values outright, so this also
        // proves a hostile clientChain can never break session creation.
        Map<String, String> headers = ClientHeader.withHop(
                Map.of(ClientHeader.HEADER_NAME, "client=cli\r\nX-Evil: 1"));

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create("https://api.example.com/sql/session"));
        headers.forEach(builder::header);
        HttpRequest request = builder.build();

        String value = request.headers().firstValue(ClientHeader.HEADER_NAME).orElseThrow();
        assertTrue(value.contains("client=jdbc;ver="), value);
    }

    @Test
    void withHopHandlesNullAndImmutableHeaderMaps() {
        assertTrue(ClientHeader.withHop(null).get(ClientHeader.HEADER_NAME)
                .startsWith("client=jdbc;"));
        assertTrue(ClientHeader.withHop(Map.of("X-API-Key", "secret"))
                .get(ClientHeader.HEADER_NAME).startsWith("client=jdbc;"));
    }
}
