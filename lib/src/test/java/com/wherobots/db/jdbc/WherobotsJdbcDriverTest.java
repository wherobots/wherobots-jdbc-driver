package com.wherobots.db.jdbc;


import org.junit.jupiter.api.Test;

import java.util.Map;

class WherobotsJdbcDriverTest {

    @Test
    void getUserAgentHeader() {
        System.setProperty("java.version", "1");
        System.setProperty("os.name", "os1");
        WherobotsJdbcDriver driver = new WherobotsJdbcDriver();
        Map<String, String> header = driver.getUserAgentHeader();
        assert header.containsKey("User-Agent");
        String user_agent = header.get("User-Agent");
        assert user_agent.equals("wherobots-jdbc-driver/unknown os/os1 java/1");
    }
}