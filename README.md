# Wherobots Spatial SQL JDBC Driver

This library provides a JDBC driver implementation for the Wherobots
Spatial SQL API. It allows you to build Java/Scala/Kotlin applications
that can interact with Wherobots and leverage the Spatial SQL and
geospatial analytics capabilities of WherobotsDB.

This JDBC driver is also directly usable from database applications like
JetBrains's DataGrip or DBeaver.

## Usage

### As a library

```gradle
dependencies {
  implementation 'com.wherobots.jdbc:wherobots-jdbc-driver:0.1.0'
}
```

```java
DriverManager.setLogWriter(new PrintWriter(System.out));
DriverManager.registerDriver(new WherobotsJdbcDriver());

Properties props = new Properties();
props.put(WherobotsJdbcDriver.API_KEY_PROP, apiKey);

logger.info("Connecting to Wherobots SQL API with properties: {}", props);

try (Connection conn = DriverManager.getConnection("jdbc:wherobots://api.cloud.wherobots.com", props)) {
    try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
    }
}
```

### In DataGrip

TODO: add instructions for configuring a database connection with the
Wherobots JDBC driver.

## Connection parameters

The following parameters are available, controlled by properties passed
to the JDBC driver:

* `apiKey`: the API key to authenticate with Wherobots Cloud;
* `runtime`: the `Runtime` type (as a string) to instantiate;
* `region`: the `Region` name (as a string) to spawn the SQL session
  runtime into (defaults to `aws-us-west-2`);
* `version`: the version of WherobotsDB to use (defaults to `latest`);
* `sessionType`: the type of session (`single` or `multi`);
* `forceNew`: whether to force the creation a new SQL session runtime
  for this connection (defaults to `false`);

## Release

To publish a new release, you need an account on [Sonatype's Central
Portal](https://central.sonatype.com) with permissions to publish into
the `com.wherobots` namespace. From your account, create an API key,
which gets you a user token and secret key pair:

```sh
export OSSRH_USERNAME=<user token>
export OSSRH_PASSWORD=<secret key>
```

You also need a GnuPG key, loaded up in `gpg-agent`, for signing the
release artifacts.

Then, after setting the new version in `lib/build.gradle`, publish with:

```sh
$ ./gradlew clean publishToCentralPortal
```

You can then check the status and validation of thsi deployment at
https://central.sonatype.com/publishing/deployments
