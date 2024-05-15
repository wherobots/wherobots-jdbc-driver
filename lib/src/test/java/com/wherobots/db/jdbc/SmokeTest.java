package com.wherobots.db.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class SmokeTest {
    public static void main(String[] args) throws Exception {
        DriverManager.setLogWriter(new PrintWriter(System.out));
        DriverManager.registerDriver(new WherobotsJdbcDriver());

        String sql = """
            SELECT
                id,
                names['primary'],
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

        try (Connection conn = DriverManager.getConnection("jdbc:wherobots://api.staging.wherobots.com", props)) {
            Statement stmt = conn.createStatement();
            try (ResultSet result = stmt.executeQuery(sql)) {
                while (result.next()) {
                    System.out.printf("%s %s %s %d%n",
                            result.getString(1),
                            result.getString(2),
                            result.getString(3),
                            result.getInt(4)
                    );
                }
            }
        }
    }
}
