package com.wherobots.db.jdbc;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helpers for the shared, cross-client {@code X-Wherobots-Client} attribution
 * header.
 * <p>
 * The header is an ordered, append-only, comma-separated list of hops, where
 * each hop has the form {@code client=<token>} optionally followed by
 * {@code ;key=value} parameters ({@code ver}, {@code plat}, {@code cmd}). The
 * <em>leftmost</em> hop is the origin client and each component appends its own
 * hop <em>on the right</em>. This driver's hop is
 * {@code client=jdbc;ver=<driver version>;plat=<os name>}.
 * </p>
 * <p>
 * The driver is normally an origin client and emits a single hop. A caller that
 * is itself acting on behalf of an upstream Wherobots client (a BI tool
 * integration, a service embedding the driver) can pass that upstream chain
 * through the {@code clientChain} connection property; it is sanitized and kept
 * to the left of this driver's hop.
 * </p>
 * <p>
 * The header is <strong>advisory only</strong>: it is client-asserted, used for
 * attribution and analytics, and must never influence authentication or
 * authorization.
 * </p>
 */
public final class ClientHeader {

    /** Canonical name of the shared client-chain header. */
    public static final String HEADER_NAME = "X-Wherobots-Client";

    /** This driver's stable token in the shared client vocabulary. */
    public static final String CLIENT_TOKEN = "jdbc";

    /**
     * Maximum size of the rendered header value, in UTF-8 bytes. A larger value
     * is treated as malformed by the server-side parser, which then attributes
     * the request to {@code unknown} — so we never emit one.
     */
    public static final int MAX_HEADER_BYTES = 512;

    /** Recorded when the driver version cannot be determined. */
    static final String UNKNOWN_VERSION = "unknown";

    /**
     * Individual tokens are kept well under 64 characters, per the header
     * convention.
     */
    private static final int MAX_PARAM_LENGTH = 63;

    /**
     * Characters allowed in a parameter value we render ourselves. Anything
     * else — including the {@code ,} and {@code ;} delimiters and any control
     * character — collapses to a single hyphen.
     */
    private static final Pattern UNSAFE_PARAM_CHARS = Pattern.compile("[^A-Za-z0-9._+-]+");

    /**
     * Characters allowed in a caller-supplied upstream chain. The chain carries
     * its own {@code ,} / {@code ;} / {@code =} grammar, so those are kept;
     * everything outside this set (notably CR, LF and other control characters,
     * which would allow header injection) becomes an underscore.
     */
    private static final Pattern UNSAFE_CHAIN_CHARS = Pattern.compile("[^A-Za-z0-9._+:/@=;, -]");

    private static final Pattern REPEATED_WHITESPACE = Pattern.compile("\\s+");

    private ClientHeader() {}

    /**
     * Returns a copy of {@code headers} carrying a well-formed
     * {@code X-Wherobots-Client} value.
     * <p>
     * Any existing entry for the header — matched case-insensitively, since HTTP
     * header names are case-insensitive — is treated as the upstream chain and
     * collapsed into the canonical key, so the request carries exactly one such
     * header with this driver's hop appended on the right.
     * </p>
     *
     * @param headers the outgoing headers; may be null or immutable, and is
     *                never modified
     * @return a new mutable map with the attribution header set
     */
    public static Map<String, String> withHop(Map<String, String> headers) {
        Map<String, String> result = new LinkedHashMap<>();
        String upstream = null;
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(HEADER_NAME)) {
                    upstream = entry.getValue();
                    continue;
                }
                result.put(entry.getKey(), entry.getValue());
            }
        }
        result.put(HEADER_NAME, value(upstream));
        return result;
    }

    /**
     * Renders the full header value: the sanitized upstream chain, if any,
     * followed by this driver's own hop.
     *
     * @param upstreamChain an upstream chain to prepend, or null
     * @return a header value that always fits within {@link #MAX_HEADER_BYTES}
     */
    public static String value(String upstreamChain) {
        String hop = hop(driverVersion(), System.getProperty("os.name"));
        String chain = sanitizeChain(upstreamChain);
        if (chain.isEmpty()) {
            return hop;
        }

        String combined = chain + ", " + hop;
        if (utf8Length(combined) > MAX_HEADER_BYTES) {
            // Truncating mid-chain would corrupt the grammar, and an oversized
            // value makes the whole header unparseable — so drop the untrusted
            // upstream chain and keep our own attributable hop.
            return hop;
        }
        return combined;
    }

    /**
     * Renders this driver's single hop, {@code client=jdbc;ver=<version>} plus
     * {@code ;plat=<platform>} when the platform is known. An unavailable
     * version degrades to {@code ver=unknown} rather than dropping the hop.
     */
    static String hop(String version, String platform) {
        String ver = sanitizeParam(version);
        String plat = sanitizeParam(platform == null ? null : platform.toLowerCase(Locale.ROOT));

        StringBuilder hop = new StringBuilder("client=").append(CLIENT_TOKEN);
        hop.append(";ver=").append(ver.isEmpty() ? UNKNOWN_VERSION : ver);
        if (!plat.isEmpty()) {
            hop.append(";plat=").append(plat);
        }
        return hop.toString();
    }

    /**
     * The driver version as recorded in the JAR manifest, or null when the
     * driver runs from a plain class directory (tests, IDE runs) and there is no
     * manifest to read.
     */
    static String driverVersion() {
        Package pkg = ClientHeader.class.getPackage();
        return pkg == null ? null : pkg.getImplementationVersion();
    }

    /**
     * Makes a caller-supplied chain safe to send: drops characters that could
     * corrupt the grammar or inject a header, normalizes whitespace, and removes
     * empty hops. A chain that cannot fit in the header at all yields an empty
     * string.
     */
    static String sanitizeChain(String chain) {
        if (chain == null) {
            return "";
        }

        String cleaned = UNSAFE_CHAIN_CHARS.matcher(chain).replaceAll("_");
        List<String> hops = new ArrayList<>();
        for (String hop : cleaned.split(",")) {
            String normalized = REPEATED_WHITESPACE.matcher(hop).replaceAll(" ").trim();
            if (!normalized.isEmpty()) {
                hops.add(normalized);
            }
        }

        String joined = String.join(", ", hops);
        return utf8Length(joined) > MAX_HEADER_BYTES ? "" : joined;
    }

    private static String sanitizeParam(String value) {
        if (value == null) {
            return "";
        }
        String sanitized = UNSAFE_PARAM_CHARS.matcher(value.trim()).replaceAll("-");
        // Leading/trailing separators carry no information and read as noise.
        sanitized = sanitized.replaceAll("^-+", "").replaceAll("-+$", "");
        return sanitized.length() > MAX_PARAM_LENGTH
                ? sanitized.substring(0, MAX_PARAM_LENGTH)
                : sanitized;
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
