package com.wherobots.db.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import com.wherobots.db.jdbc.models.Store;
import com.wherobots.db.jdbc.models.StoreResult;
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
              latitude,
              longitude
            FROM
              wherobots_open_data.foursquare.places
            WHERE
              country = 'US'
              AND LOWER(name) = 'starbucks'
            LIMIT 10
        """;

        Properties props = new Properties();
        props.put(WherobotsJdbcDriver.API_KEY_PROP, args[0]);
        props.put(WherobotsJdbcDriver.SESSION_TYPE_PROP, SessionType.SINGLE.name());
        props.put(WherobotsJdbcDriver.FORCE_NEW_PROP, "false");

        logger.info("Connecting to Wherobots SQL API with properties: {}", props);

        try (Connection conn = DriverManager.getConnection("jdbc:wherobots://api.cloud.wherobots.com", props)) {
            try (Statement stmt = conn.createStatement()) {
                // Configure store to get a presigned URL for download
                WherobotsStatement wstmt = stmt.unwrap(WherobotsStatement.class);
                wstmt.setStore(Store.forDownload());

                boolean hasResult = stmt.execute(sql);

                // Print store result if available
                StoreResult storeResult = wstmt.getStoreResult();
                if (storeResult != null) {
                    logger.info("Results stored at: {} (size: {} bytes)", storeResult.resultUri(), storeResult.size());
                }

                if (!hasResult) {
                    return;
                }

                try (ResultSet result = stmt.getResultSet()) {
                    int i = 0;
                    while (result.next()) {
                        i++;
                        System.out.printf("%4d: %.5f\t%.5f%n",
                                i,
                                result.getDouble("latitude"),
                                result.getDouble("longitude")
                        );
                    }
                }
            }
        }
    }
}
