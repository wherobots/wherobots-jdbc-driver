package com.wherobots.db.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wherobots.db.SessionType;

public class SmokeTest {
    private static final Logger logger = LoggerFactory.getLogger(SmokeTest.class);

    public static void main(String[] args) throws Exception {
        DriverManager.setLogWriter(new PrintWriter(System.out));
        DriverManager.registerDriver(new WherobotsJdbcDriver());

        String sql = """
            SELECT
                id,
                names['primary'] AS name,
                geometry,
                population
            FROM
                wherobots_open_data.overture_2024_02_15.admins_locality
            WHERE localityType = 'country'
            SORT BY population DESC
            LIMIT 10
        """;

        Properties props = new Properties();
        props.put(WherobotsJdbcDriver.API_KEY_PROP, args[0]);
        props.put(WherobotsJdbcDriver.SESSION_TYPE_PROP, SessionType.SINGLE.name());
        props.put(WherobotsJdbcDriver.FORCE_NEW_PROP, "false");

        logger.info("Connecting to Wherobots SQL API with properties: {}", props);

        try (Connection conn = DriverManager.getConnection("jdbc:wherobots://api.staging.wherobots.com", props)) {
            try (Statement stmt = conn.createStatement()) {
                new Thread(() -> {
                    try {
                        System.out.println("Cancelling query in 2s!");
                        Thread.sleep(2000L);
                        stmt.cancel();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();

                boolean hasResult = stmt.execute(sql);
                if (!hasResult) {
                    return;
                }

                try (ResultSet result = stmt.getResultSet()) {
                    while (result.next()) {
                        System.out.printf("%s: %s\t%s\t%12d%n",
                                result.getString("id"),
                                result.getString("name"),
                                result.getString("geometry"),
                                result.getInt("population")
                        );
                    }
                }
            }
        }
    }
}
