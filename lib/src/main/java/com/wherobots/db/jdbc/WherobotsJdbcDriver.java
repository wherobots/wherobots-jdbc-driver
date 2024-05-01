package com.wherobots.db.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class WherobotsJdbcDriver implements Driver {

    private static final Logger logger = Logger.getLogger(WherobotsJdbcDriver.class.getName());

    private static final String JDBC_PREFIX = "jdbc:";
    private static final String URL_PREFIX = "jdbc:wherobots://";
    public static final String RUNTIME_PROP = "runtime";
    public static final String REGION_PROP = "region";

    public static final String DEFAULT_ENDPOINT = "cloud.wherobots.com";
    public static final Runtime DEFAULT_RUNTIME = Runtime.SEDONA;
    public static final Region DEFAULT_REGION = Region.AWS_US_WEST_2;

    private static final Logger PARENT_LOGGER = Logger.getLogger(WherobotsJdbcDriver.class.getPackageName());
    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            URIBuilder parsed = new URIBuilder(url.substring(JDBC_PREFIX.length()));
            String host = DEFAULT_ENDPOINT;
            if (StringUtils.isNotBlank(parsed.getHost())) {
                host = parsed.getHost();
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

            logger.info("Connecting to " + host + " with runtime " + runtime.name + " in region " + region.name);
            return new WherobotsJdbcConnection(WherobotsSessionSupplier.create(host, runtime, region).get());
        } catch (URISyntaxException e) {
            throw new SQLException("Invalid URL: " + url, e);
        }
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
    public Logger getParentLogger() {
        return PARENT_LOGGER;
    }
}
