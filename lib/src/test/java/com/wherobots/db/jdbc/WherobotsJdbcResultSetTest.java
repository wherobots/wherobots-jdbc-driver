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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
    void getDataTypesNonNested() throws Exception {
        String sql = """
                SELECT
                  CAST('hello' AS STRING)           AS str_col,
                  CAST(TRUE AS BOOLEAN)             AS bool_col,
                  CAST(42 AS TINYINT)               AS byte_col,
                  CAST(1234 AS SMALLINT)            AS short_col,
                  CAST(100000 AS INT)               AS int_col,
                  CAST(9999999999 AS BIGINT)         AS long_col,
                  CAST(3.14 AS FLOAT)               AS float_col,
                  CAST(2.718281828 AS DOUBLE)        AS double_col,
                  CAST(X'DEADBEEF' AS BINARY)        AS bytes_col,
                  CAST('2025-06-15' AS DATE)         AS date_col,
                  CAST('2025-06-15T10:30:00' AS TIMESTAMP) AS timestamp_col
                """;

        try (Statement stmt = connection.createStatement()) {
            assertTrue(stmt.execute(sql));

            try (ResultSet rs = stmt.getResultSet()) {
                assertTrue(rs.next(), "expected one row");

                assertEquals("hello", rs.getString("str_col"));
                assertTrue(rs.getBoolean("bool_col"));
                assertEquals((byte) 42, rs.getByte("byte_col"));
                assertEquals((short) 1234, rs.getShort("short_col"));
                assertEquals(100000, rs.getInt("int_col"));
                assertEquals(9999999999L, rs.getLong("long_col"));
                assertEquals(3.14f, rs.getFloat("float_col"), 0.01f);
                assertEquals(2.718281828, rs.getDouble("double_col"), 0.000001);
                assertArrayEquals(new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF },
                        rs.getBytes("bytes_col"));
                assertEquals(java.sql.Date.valueOf("2025-06-15"), rs.getDate("date_col"));
                assertEquals(
                        Timestamp.from(Instant.parse("2025-06-15T10:30:00Z")),
                        rs.getTimestamp("timestamp_col"));

                assertFalse(rs.next(), "expected only one row");
            }
        }
    }

    @Test
    void getNestedTypes() throws Exception {
        String sql = """
                SELECT
                  array(1, 2, 3)                          AS array_col,
                  map('a', 1, 'b', 2)                     AS map_col,
                  named_struct('x', 10, 'y', 'hello')     AS struct_col
                """;

        try (Statement stmt = connection.createStatement()) {
            assertTrue(stmt.execute(sql));

            try (ResultSet rs = stmt.getResultSet()) {
                assertTrue(rs.next(), "expected one row");

                // Array
                Object arrayVal = rs.getObject("array_col");
                assertInstanceOf(List.class, arrayVal);
                assertEquals(List.of(1, 2, 3), arrayVal);

                // Map
                Object mapVal = rs.getObject("map_col");
                assertInstanceOf(Map.class, mapVal);
                assertEquals(Map.of("a", 1, "b", 2), mapVal);

                // Struct
                Object structVal = rs.getObject("struct_col");
                assertNotNull(structVal);
                String structStr = structVal.toString();
                assertTrue(structStr.contains("10"), "struct should contain field value 10");
                assertTrue(structStr.contains("hello"), "struct should contain field value 'hello'");

                assertFalse(rs.next(), "expected only one row");
            }
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
