package com.wherobots.db.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class WherobotsJdbcDriver implements Driver {

    private static final String JDBC_PREFIX = "jdbc:";
    private static final String URL_PREFIX = "jdbc:wherobots://";

    public static final String API_KEY_PROP = "apiKey";
    public static final String TOKEN_PROP = "token";
    public static final String RUNTIME_PROP = "runtime";
    public static final String REGION_PROP = "region";
    public static final String WS_URI_PROP = "wsUri";

    public static final String DEFAULT_ENDPOINT = "api.cloud.wherobots.com";
    public static final String STAGING_ENDPOINT = "api.staging.wherobots.com";

    public static final Runtime DEFAULT_RUNTIME = Runtime.SEDONA;
    public static final Region DEFAULT_REGION = Region.AWS_US_WEST_2;

    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;

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

        Map<String, String> headers = getAuthHeaders(info);

        String wsUriString = info.getProperty(WS_URI_PROP);
        if (StringUtils.isNotBlank(wsUriString)) {
            try {
                URI wsUri = new URI(wsUriString);
                return new WherobotsJdbcConnection(WherobotsSessionSupplier.create(wsUri, headers));
            } catch (URISyntaxException e) {
                throw new SQLException("Invalid WebSocket URI: " + wsUriString, e);
            }
        }

        return new WherobotsJdbcConnection(WherobotsSessionSupplier.create(host, runtime, region, headers));
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
