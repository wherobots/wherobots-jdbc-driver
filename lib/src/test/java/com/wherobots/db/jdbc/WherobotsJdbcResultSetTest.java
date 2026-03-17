package com.wherobots.db.jdbc;

import com.wherobots.db.SessionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "WHEROBOTS_API_KEY", matches = ".+")
class WherobotsJdbcResultSetTest {

    private static Connection connection;

    @BeforeAll
    static void setUp() throws Exception {
        DriverManager.registerDriver(new WherobotsJdbcDriver());

        Properties props = new Properties();
        props.put(WherobotsJdbcDriver.API_KEY_PROP, System.getenv("WHEROBOTS_API_KEY"));
        props.put(WherobotsJdbcDriver.SESSION_TYPE_PROP, SessionType.SINGLE.name());
        props.put(WherobotsJdbcDriver.FORCE_NEW_PROP, "false");

        String host = System.getenv().getOrDefault("WHEROBOTS_HOST", "api.cloud.wherobots.com");
        connection = DriverManager.getConnection("jdbc:wherobots://" + host, props);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void queryFoursquarePlaces() throws Exception {
        String sql = """
            SELECT
              latitude,
              longitude
            FROM
              wherobots_open_data.foursquare.places
            WHERE
              country = 'US'
              AND LOWER(name) = 'starbucks'
            LIMIT 10
            """;

        try (Statement stmt = connection.createStatement()) {
            assertTrue(stmt.execute(sql));

            try (ResultSet rs = stmt.getResultSet()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    double lat = rs.getDouble("latitude");
                    double lon = rs.getDouble("longitude");
                    assertFalse(Double.isNaN(lat), "latitude should be a valid number");
                    assertFalse(Double.isNaN(lon), "longitude should be a valid number");
                }
                assertEquals(10, count, "expected 10 rows");
            }
        }
    }
}
