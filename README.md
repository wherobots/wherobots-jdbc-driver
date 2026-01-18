# Wherobots JDBC Driver

[![Maven Central](https://img.shields.io/maven-central/v/com.wherobots.jdbc/wherobots-jdbc-driver.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/com.wherobots.jdbc/wherobots-jdbc-driver)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/)

A JDBC driver for [Wherobots Spatial SQL](https://www.wherobots.com), enabling
Java, Scala, and Kotlin applications to interact with WherobotsDB and leverage
its powerful geospatial analytics capabilities.

This driver is also compatible with database tools like [JetBrains
DataGrip](https://www.jetbrains.com/datagrip/) and
[DBeaver](https://dbeaver.io/).

## Installation

### Gradle

```gradle
dependencies {
    implementation 'com.wherobots.jdbc:wherobots-jdbc-driver:0.2.0'
}
```

### Maven

```xml
<dependency>
    <groupId>com.wherobots.jdbc</groupId>
    <artifactId>wherobots-jdbc-driver</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Quick Start

```java
import com.wherobots.db.jdbc.WherobotsJdbcDriver;
import java.sql.*;
import java.util.Properties;

public class Example {
    public static void main(String[] args) throws SQLException {
        // Register the driver
        DriverManager.registerDriver(new WherobotsJdbcDriver());

        // Configure connection properties
        Properties props = new Properties();
        props.put("apiKey", System.getenv("WHEROBOTS_API_KEY"));
        props.put("runtime", "SMALL");
        props.put("region", "AWS_US_WEST_2");

        // Connect and execute a query
        String url = "jdbc:wherobots://api.cloud.wherobots.com";
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT id, ST_AsText(geometry) as geom FROM wherobots_open_data.overture.admins LIMIT 10")) {
            while (rs.next()) {
                System.out.printf("%s: %s%n", rs.getString("id"), rs.getString("geom"));
            }
        }
    }
}
```

## Connection Parameters

Configure the driver using properties passed to `DriverManager.getConnection()`:

### Authentication

| Property | Description |
|----------|-------------|
| `apiKey` | Your Wherobots Cloud API key |
| `token`  | Alternative: a bearer token for authentication |

### Session Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `runtime` | `Runtime` | `TINY` | The runtime size to use (see [Runtimes](#runtimes)) |
| `region` | `Region` | `AWS_US_WEST_2` | The cloud region to run in (see [Regions](#regions)) |
| `version` | `String` | _(latest)_ | Pin to a specific WherobotsDB runtime version |
| `sessionType` | `SessionType` | `MULTI` | `SINGLE` or `MULTI` concurrent connections |
| `forceNew` | `boolean` | `false` | Force creation of a new session instead of reusing an existing one |
| `wsUri` | `String` | _(none)_ | Connect directly to a WebSocket URI (advanced) |

### Result Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `format` | `DataFormat` | `arrow` | Result format: `arrow` or `json` |
| `compression` | `DataCompression` | `zstd` | Compression: `none`, `lz4`, or `zstd` |
| `geometry` | `GeometryRepresentation` | _(none)_ | Geometry output: `wkt`, `wkb`, `ewkt`, `ewkb`, or `geojson` |

<details>
<summary><h3>Runtimes</h3></summary>

The `runtime` property accepts the following values:

| Runtime | Description |
|---------|-------------|
| `MICRO` | Micro instance |
| `TINY` | Tiny instance (default) |
| `SMALL` | Small instance |
| `MEDIUM` | Medium instance |
| `LARGE` | Large instance |
| `X_LARGE` | Extra-large instance |
| `XX_LARGE` | 2x-large instance |
| `MEDIUM_HIMEM` | Medium high-memory instance |
| `LARGE_HIMEM` | Large high-memory instance |
| `X_LARGE_HIMEM` | Extra-large high-memory instance |
| `XX_LARGE_HIMEM` | 2x-large high-memory instance |
| `XXXX_LARGE_HIMEM` | 4x-large high-memory instance |
| `MICRO_A10_GPU` | Micro GPU instance |
| `TINY_A10_GPU` | Tiny GPU instance |
| `SMALL_GPU` | Small GPU instance |
| `MEDIUM_GPU` | Medium GPU instance |

</details>

<details>
<summary><h3>Regions</h3></summary>

The `region` property accepts the following values:

| Region | Location |
|--------|----------|
| `AWS_US_WEST_2` | US West (Oregon) - default |
| `AWS_US_EAST_1` | US East (N. Virginia) |
| `AWS_EU_WEST_1` | EU (Ireland) |
| `AWS_AP_SOUTH_1` | Asia Pacific (Mumbai) |

</details>

## Using with DataGrip

1. Download the latest driver JAR from [Maven
   Central](https://central.sonatype.com/artifact/com.wherobots.jdbc/wherobots-jdbc-driver)
2. In DataGrip, go to **Database** → **New** → **Driver**
3. Add the downloaded JAR file
4. Set the driver class to `com.wherobots.db.jdbc.WherobotsJdbcDriver`
5. Create a new connection using this driver with URL
   `jdbc:wherobots://api.cloud.wherobots.com`
6. Add your `apiKey` in the connection properties

## Storing Results to Cloud Storage

The driver supports storing query results directly to cloud storage and
returning a presigned URL for download. This is useful for large result sets
that you want to process outside of JDBC.

This feature is exposed as a Wherobots-specific extension, accessible via
`unwrap()`:

```java
import com.wherobots.db.StorageFormat;
import com.wherobots.db.jdbc.WherobotsStatement;
import com.wherobots.db.jdbc.models.Store;
import com.wherobots.db.jdbc.models.StoreResult;

try (Statement stmt = conn.createStatement()) {
    // Unwrap to access Wherobots-specific features
    WherobotsStatement wstmt = stmt.unwrap(WherobotsStatement.class);

    // Configure to store results and get a presigned URL
    wstmt.setStore(Store.forDownload());

    // Execute the query
    wstmt.execute("SELECT * FROM my_table");

    // Get the presigned URL and file size
    StoreResult result = wstmt.getStoreResult();
    System.out.println("Download URL: " + result.resultUri());
    System.out.println("File size: " + result.size() + " bytes");
}
```

### Store Options

The `Store` class provides factory methods for creating store configurations:

| Factory Method | Description |
|----------------|-------------|
| `Store.forDownload()` | Single parquet file with presigned URL (most common) |
| `Store.forDownload(format)` | Single file with presigned URL in specified format |

The `StorageFormat` enum supports: `parquet`, `csv`, and `geojson`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and release
instructions.

## License

This project is licensed under the Apache License 2.0 - see the
[LICENSE](LICENSE) file for details.
