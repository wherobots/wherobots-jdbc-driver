package com.wherobots.db.jdbc;

import com.wherobots.db.Region;
import com.wherobots.db.Runtime;
import com.wherobots.db.SessionType;
import com.wherobots.db.jdbc.session.WherobotsSession;
import com.wherobots.db.jdbc.session.WherobotsSessionSupplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WherobotsJdbcDriver implements Driver {

    public static final String DRIVER_NAME = "wherobots";
    public static final int MAJOR_VERSION = 0;
    public static final int MINOR_VERSION = 1;

    private static final String JDBC_PREFIX = "jdbc:";
    public static final String URL_PREFIX = JDBC_PREFIX + DRIVER_NAME + "://";

    public static final String API_KEY_PROP = "apiKey";
    public static final String TOKEN_PROP = "token";

    /**
     * The compute runtime to use. Accepts a {@link com.wherobots.db.Runtime}
     * enum name or a raw string; the value is passed to the API as-is. Override
     * the default runtime set for your organization — only set this if you need
     * a specific runtime instead of the one your administrator has configured.
     * When omitted, your organization's default runtime is used.
     */
    public static final String RUNTIME_PROP = "runtime";

    /**
     * The compute region to run in. Accepts a {@link com.wherobots.db.Region}
     * enum name or a raw string (e.g. a BYOC region such as
     * {@code byoc-acme-us-east-1}); the value is passed to the API as-is.
     * Override the default region set for your organization — only set this if
     * you intend to use a specific region instead of the one your administrator
     * has configured. When omitted, your organization's default region is used.
     */
    public static final String REGION_PROP = "region";
    public static final String VERSION_PROP = "version";
    public static final String SESSION_TYPE_PROP = "sessionType";
    public static final String FORCE_NEW_PROP = "forceNew";
    public static final String SHUTDOWN_AFTER_INACTIVE_SECONDS_PROP = "shutdownAfterInactiveSeconds";
    public static final String WS_URI_PROP = "wsUri";

    // Results format; one of {@link DataFormat}
    public static final String FORMAT_PROP = "format";

    // Results compression codec; one of {@link DataCompression}
    public static final String COMPRESSION_PROP = "compression";

    // Geometry representation format; one of {@link GeometryRepresentation}
    public static final String GEOMETRY_PROP = "geometry";

    public static final String DEFAULT_ENDPOINT = "api.cloud.wherobots.com";
    public static final String STAGING_ENDPOINT = "api.staging.wherobots.com";

    public static final SessionType DEFAULT_SESSION_TYPE = SessionType.MULTI;
    public static final boolean DEFAULT_FORCE_NEW = false;

    public Map<String, String> getUserAgentHeader() {
        String javaVersion = System.getProperty("java.version");
        String osName = System.getProperty("os.name");
        String packageVersion = getClass().getPackage().getImplementationVersion();
        if (packageVersion == null) {
            packageVersion = "unknown";
        }
        String userAgent = String.format("wherobots-jdbc-driver/%s os/%s java/%s",
                packageVersion, osName, javaVersion);
        return Map.of("User-Agent", userAgent);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String host = DEFAULT_ENDPOINT;
        try {
            URIBuilder parsed = new URIBuilder(url.substring(JDBC_PREFIX.length()));
            if (StringUtils.isNotBlank(parsed.getHost())) {
                host = parsed.getHost();
            }
        } catch (URISyntaxException e) {
            throw new SQLException("Invalid URL: " + url, e);
        }

        // Resolve region/runtime to the value sent to the API: a known enum
        // constant name (e.g. AWS_US_WEST_2) maps to its API value for backward
        // compatibility, while any other string (an API value or a BYOC region)
        // is passed through untouched — so new or BYOC values work without a
        // driver release. When omitted (null), the API applies the
        // organization's configured default region/runtime.
        String runtime = Runtime.toApiValue(StringUtils.trimToNull(info.getProperty(RUNTIME_PROP)));
        String region = Region.toApiValue(StringUtils.trimToNull(info.getProperty(REGION_PROP)));

        String version = null;
        String givenVersion = info.getProperty(VERSION_PROP);
        if (StringUtils.isNotBlank(givenVersion)) {
            version = givenVersion;
        }

        SessionType sessionType = DEFAULT_SESSION_TYPE;
        String sessionTypeName = info.getProperty(SESSION_TYPE_PROP);
        if (StringUtils.isNotBlank(sessionTypeName)) {
            sessionType = SessionType.valueOf(sessionTypeName);
        }

        boolean forceNew = DEFAULT_FORCE_NEW;
        String forceNewStr = info.getProperty(FORCE_NEW_PROP);
        if (StringUtils.isNotBlank(forceNewStr)) {
            forceNew = Boolean.parseBoolean(forceNewStr);
        }

        Integer shutdownAfterInactiveSeconds = null;
        String shutdownStr = info.getProperty(SHUTDOWN_AFTER_INACTIVE_SECONDS_PROP);
        if (StringUtils.isNotBlank(shutdownStr)) {
            shutdownAfterInactiveSeconds = Integer.parseInt(shutdownStr);
        }

        Map<String, String> headers = new HashMap<>(getAuthHeaders(info));
        headers.putAll(getUserAgentHeader());
        WherobotsSession session;

        String wsUriString = info.getProperty(WS_URI_PROP);
        if (StringUtils.isNotBlank(wsUriString)) {
            try {
                URI wsUri = new URI(wsUriString);
                session = WherobotsSessionSupplier.create(wsUri, headers);
            } catch (URISyntaxException e) {
                throw new SQLException("Invalid WebSocket URI: " + wsUriString, e);
            }
        } else {
            session = WherobotsSessionSupplier.create(
                    host,
                    runtime,
                    region,
                    version,
                    sessionType,
                    forceNew,
                    shutdownAfterInactiveSeconds,
                    headers
            );
        }

        return new WherobotsJdbcConnection(session, info);
    }

    private Map<String, String> getAuthHeaders(Properties info) {
        String token = info.getProperty(TOKEN_PROP);
        if (StringUtils.isNotBlank(token)) {
            return Map.of("Authorization", "Bearer " + token);
        }

        String apiKey = info.getProperty(API_KEY_PROP);
        if (StringUtils.isNotBlank(apiKey)) {
            return Map.of("X-API-Key", apiKey);
        }

        return Collections.emptyMap();
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        // TODO: Run JDBC compliance test and evaluate.
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger(WherobotsJdbcDriver.class.getName());
    }
}
