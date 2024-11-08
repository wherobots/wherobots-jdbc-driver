package com.wherobots.db.jdbc;

import com.wherobots.db.Region;
import com.wherobots.db.Runtime;
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
    public static final String RUNTIME_PROP = "runtime";
    public static final String REGION_PROP = "region";
    public static final String REUSE_SESSION_PROP = "reuseSession";
    public static final String WS_URI_PROP = "wsUri";

    // Results format; one of {@link DataFormat}
    public static final String FORMAT_PROP = "format";

    // Results compression codec; one of {@link DataCompression}
    public static final String COMPRESSION_PROP = "compression";

    // Geometry representation format; one of {@link GeometryRepresentation}
    public static final String GEOMETRY_PROP = "geometry";

    public static final String DEFAULT_ENDPOINT = "api.cloud.wherobots.com";
    public static final String STAGING_ENDPOINT = "api.staging.wherobots.com";

    public static final Runtime DEFAULT_RUNTIME = Runtime.TINY;
    public static final Region DEFAULT_REGION = Region.AWS_US_WEST_2;
    public static final boolean DEFAULT_REUSE_SESSION = true;

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

        Runtime runtime = DEFAULT_RUNTIME;
        String runtimeName = info.getProperty(RUNTIME_PROP);
        if (StringUtils.isNotBlank(runtimeName)) {
            runtime = Runtime.valueOf(runtimeName);
        }

        Region region = DEFAULT_REGION;
        String regionName = info.getProperty(REGION_PROP);
        if (StringUtils.isNotBlank(regionName)) {
            region = Region.valueOf(regionName);
        }

        boolean reuse = DEFAULT_REUSE_SESSION;
        String reuseSession = info.getProperty(REUSE_SESSION_PROP);
        if (StringUtils.isNotBlank(reuseSession)) {
            reuse = Boolean.parseBoolean(reuseSession);
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
            session = WherobotsSessionSupplier.create(host, runtime, region, reuse, headers);
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
