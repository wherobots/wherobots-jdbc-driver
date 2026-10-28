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

Releases run in CI through `.github/workflows/release.yml`.

1. In a PR, bump `version` in `lib/build.gradle`.
2. Merge it. The workflow runs on any push to `main` that changes
   `lib/build.gradle`. If tag `v<version>` does not exist yet, it signs the
   artifacts, runs `./gradlew clean build shadowJar publishToCentralPortal`,
   and creates GitHub release `v<version>` (which creates the tag) with the
   shaded `*-all.jar` attached.
3. Check the deployment at
   https://central.sonatype.com/publishing/deployments

Don't publish from your machine or push the tag by hand. If tag
`v<version>` already exists, the workflow skips the release.
