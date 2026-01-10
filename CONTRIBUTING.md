# Contributing to Wherobots JDBC Driver

## Development Setup

### Prerequisites

- Java 17 or later
- Gradle 8.x (wrapper included)

### Building

```sh
./gradlew build
```

### Running Tests

```sh
./gradlew test
```

### Running the Smoke Test

The smoke test connects to a real Wherobots session:

```sh
./gradlew runSmokeTest -Papikey=<your_api_key>
```

## Releasing

### Prerequisites

1. An account on [Sonatype's Central
   Portal](https://central.sonatype.com) with permissions to publish to the
   `com.wherobots` namespace
2. A GnuPG key for signing artifacts, loaded in `gpg-agent`

### Publishing a Release

1. Update the version in `lib/build.gradle`
2. Set up your credentials:
   ```sh
   export OSSRH_USERNAME=<user token>
   export OSSRH_PASSWORD=<secret key>
   ```
3. Publish to Maven Central:
   ```sh
   ./gradlew clean publishToCentralPortal
   ```
4. Monitor the deployment at
   https://central.sonatype.com/publishing/deployments
5. Create a git tag and GitHub release for the new version
