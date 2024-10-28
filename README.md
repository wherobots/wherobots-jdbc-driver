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

### In DataGrip

TODO: add instructions for configuring a database connection with the
Wherobots JDBC driver.

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
