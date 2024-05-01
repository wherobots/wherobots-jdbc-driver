package com.wherobots.db.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class SmokeTest {
    public static void main(String[] args) throws Exception {
        DriverManager.setLogWriter(new PrintWriter(System.out));
        DriverManager.registerDriver(new WherobotsJdbcDriver());
        Connection conn = DriverManager.getConnection("jdbc:wherobots://cloud.wherobots.com", new Properties());
    }
}
