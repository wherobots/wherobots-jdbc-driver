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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "WHEROBOTS_API_KEY", matches = ".+")
class WherobotsResultSetTest {

    private static Connection connection;

    @BeforeAll
    static void setUp() throws Exception {
        DriverManager.registerDriver(new WherobotsJdbcDriver());

        String host = System.getenv().getOrDefault("WHEROBOTS_HOST", "api.cloud.wherobots.com");
        String api_key = System.getenv("WHEROBOTS_API_KEY");
        String timeoutString = System.getenv("WHEROBOTS_SHUTDOWN_AFTER_INACTIVE_SECONDS");

        Properties props = new Properties();
        props.put(WherobotsJdbcDriver.API_KEY_PROP, api_key);
        props.put(WherobotsJdbcDriver.SESSION_TYPE_PROP, SessionType.SINGLE.name());
        props.put(WherobotsJdbcDriver.FORCE_NEW_PROP, "false");
        if (timeoutString != null) {
            props.put(WherobotsJdbcDriver.SHUTDOWN_AFTER_INACTIVE_SECONDS_PROP, timeoutString);
        }

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
                assertFalse(rs.wasNull());

                assertTrue(rs.getBoolean("bool_col"));
                assertFalse(rs.wasNull());

                assertEquals((byte) 42, rs.getByte("byte_col"));
                assertFalse(rs.wasNull());

                assertEquals((short) 1234, rs.getShort("short_col"));
                assertFalse(rs.wasNull());

                assertEquals(100000, rs.getInt("int_col"));
                assertFalse(rs.wasNull());

                assertEquals(9999999999L, rs.getLong("long_col"));
                assertFalse(rs.wasNull());

                assertEquals(3.14f, rs.getFloat("float_col"), 0.01f);
                assertFalse(rs.wasNull());

                assertEquals(2.718281828, rs.getDouble("double_col"), 0.000001);
                assertFalse(rs.wasNull());

                assertArrayEquals(new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF },
                        rs.getBytes("bytes_col"));
                assertFalse(rs.wasNull());

                assertEquals(java.sql.Date.valueOf("2025-06-15"), rs.getDate("date_col"));
                assertFalse(rs.wasNull());

                assertEquals(
                        Timestamp.from(Instant.parse("2025-06-15T10:30:00Z")),
                        rs.getTimestamp("timestamp_col"));
                assertFalse(rs.wasNull());

                assertFalse(rs.next(), "expected only one row");
            }
        }
    }

    @Test
    void getDataTypesNull() throws Exception {
        String sql = """
                SELECT
                  CAST(NULL AS STRING)              AS str_col,
                  CAST(NULL AS BOOLEAN)             AS bool_col,
                  CAST(NULL AS TINYINT)             AS byte_col,
                  CAST(NULL AS SMALLINT)            AS short_col,
                  CAST(NULL AS INT)                 AS int_col,
                  CAST(NULL AS BIGINT)              AS long_col,
                  CAST(NULL AS FLOAT)               AS float_col,
                  CAST(NULL AS DOUBLE)              AS double_col,
                  CAST(NULL AS BINARY)              AS bytes_col,
                  CAST(NULL AS DATE)                AS date_col,
                  CAST(NULL AS TIMESTAMP)           AS timestamp_col
                """;

        try (Statement stmt = connection.createStatement()) {
            assertTrue(stmt.execute(sql));

            try (ResultSet rs = stmt.getResultSet()) {
                assertTrue(rs.next(), "expected one row");

                assertNull(rs.getString("str_col"));
                assertTrue(rs.wasNull());

                assertFalse(rs.getBoolean("bool_col"));
                assertTrue(rs.wasNull());

                assertEquals((byte) 0, rs.getByte("byte_col"));
                assertTrue(rs.wasNull());

                assertEquals((short) 0, rs.getShort("short_col"));
                assertTrue(rs.wasNull());

                assertEquals(0, rs.getInt("int_col"));
                assertTrue(rs.wasNull());

                assertEquals(0L, rs.getLong("long_col"));
                assertTrue(rs.wasNull());

                assertEquals(0.0f, rs.getFloat("float_col"), 0.0f);
                assertTrue(rs.wasNull());

                assertEquals(0.0, rs.getDouble("double_col"), 0.0);
                assertTrue(rs.wasNull());

                assertNull(rs.getBytes("bytes_col"));
                assertTrue(rs.wasNull());

                assertNull(rs.getDate("date_col"));
                assertTrue(rs.wasNull());

                assertNull(rs.getTimestamp("timestamp_col"));
                assertTrue(rs.wasNull());

                assertFalse(rs.next(), "expected only one row");
            }
        }
    }

    @Test
    void getNestedTypes() throws Exception {
        String sql = """
                SELECT
                  array(1, 2, 3)                          AS array_col,
                  array(
                    CAST('2025-06-15' AS DATE),
                    NULL,
                    CAST('2025-06-16' AS DATE)
                  ) AS array_of_dates_col,
                  map('a', 1, 'b', 2)                     AS map_col,
                  map('a', CAST('2025-06-15' AS DATE), 'b', null) AS map_of_dates_col,
                  named_struct('x', 10, 'y', 'hello', 'z', CAST('2025-06-15' AS DATE)) AS struct_col
                """;

        try (Statement stmt = connection.createStatement()) {
            assertTrue(stmt.execute(sql));

            try (ResultSet rs = stmt.getResultSet()) {
                assertTrue(rs.next(), "expected one row");

                // Array
                Object arrayVal = rs.getObject("array_col");
                assertInstanceOf(List.class, arrayVal);
                assertEquals(List.of(1, 2, 3), arrayVal);

                // Array of type with custom handling
                arrayVal = rs.getObject("array_of_dates_col");
                assertInstanceOf(List.class, arrayVal);
                assertEquals(
                        Arrays.asList(java.sql.Date.valueOf("2025-06-15"), null, java.sql.Date.valueOf("2025-06-16")),
                        arrayVal);

                // Map
                Object mapVal = rs.getObject("map_col");
                assertInstanceOf(Map.class, mapVal);
                assertEquals(Map.of("a", 1, "b", 2), mapVal);

                // Map of type that needs custom handling
                mapVal = rs.getObject("map_of_dates_col");
                assertInstanceOf(Map.class, mapVal);
                Map<String, Object> expectedMap = new LinkedHashMap<>();
                expectedMap.put("a", java.sql.Date.valueOf("2025-06-15"));
                expectedMap.put("b", null);
                assertEquals(expectedMap, mapVal);

                // Struct
                Object structVal = rs.getObject("struct_col");
                assertNotNull(structVal);

                assertInstanceOf(Map.class, structVal);
                Map<?, ?> structMap = (Map<?, ?>) structVal;
                assertEquals(3, structMap.size());
                assertEquals(10, structMap.get("x"));
                assertEquals("hello", structMap.get("y").toString());
                assertEquals(java.sql.Date.valueOf("2025-06-15"), structMap.get("z"));
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
