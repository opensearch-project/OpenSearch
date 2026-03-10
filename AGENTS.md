# AGENTS.md

## Repository Structure

Key components:

- `server` - Core of the OpenSearch server. Includes the distributed system framework as well as most of the search and indexing functionality.
- `plugins/*` - Optional plugins that extend interfaces defined in `server`. Plugins run in their own classloader that is a child of the `server` classloader.
- `modules/*` - Architecturally the same as plugins, but are included by default and are not uninstallable.
- `libs/*` - Libraries that can be used by `server` or any plugin or module.
- `buildSrc` - The build framework, used by this repository and all external plugin repositories. This is published as the `build-tools` artifact.
- `sandbox` - Contains `libs`, `modules`, and `plugins` that are under development. These are not included in non-snapshot builds.
- `distribution` - Builds tar, zip, rpm, and deb packages.
- `qa` - Integration tests requiring multiple modules/plugins, multi-version cluster tests, and tests in unusual configurations.
- `test` - Test framework and test fixtures used across the project. Published for external plugin testing.

## Build

JDK 21 is the minimum supported. `JAVA_HOME` must be set.

```
./gradlew assemble          # build all distributions
./gradlew localDistro       # build for local platform only
./gradlew run               # run OpenSearch from source
./gradlew generateProto     # regenerate protobuf code (if compilation errors)
```

## Testing

### Unit Testing
- Defined in `<component>/src/test` directory
- Test class names must end with "Tests"
- Base class: `OpenSearchTestCase`

### Internal Cluster Tests
- Defined in `<component>/src/internalClusterTest` directory
- Test class names must end with "IT"
- Base classes: `OpenSearchSingleNodeTestCase` (single node), `OpenSearchIntegTestCase` (multi-node)
- These tests are extremely prone to race conditions. Ensure that you are using appropriate concurrency primitives or polling when asserting on a condition that completes asynchronously.

### REST Tests
- YAML-based REST tests: `./gradlew :rest-api-spec:yamlRestTest`
- Java REST tests: `./gradlew :<module>:javaRestTest` (base class: `OpenSearchRestTestCase`)

### Running Tests

```
./gradlew check                    # all verification tasks (unit, integration, static checks)
./gradlew precommit                # precommit checks only
./gradlew internalClusterTest      # in-memory cluster integration tests only
./gradlew test                     # unit tests only

# run a specific test
./gradlew server:test --tests "*.ReplicaShardBatchAllocatorTests.testNoAsyncFetchData"
./gradlew :server:internalClusterTest --tests "org.opensearch.action.admin.ClientTimeoutIT.testNodesInfoTimeout"

# run with a specific seed for reproducibility
./gradlew test -Dtests.seed=DEADBEEF

# repeat a test N times
./gradlew server:test --tests "*.ReplicaShardBatchAllocatorTests.testNoAsyncFetchData" -Dtests.iters=N
```

### Writing Good Tests

- Prefer unit tests over multi-threaded integration tests when unit tests can provide the same test coverage.
- Use randomization for parameters not expected to affect behavior (e.g., shard count for aggregation tests), not for coverage. If different code paths exist (e.g., 1 shard vs 2+ shards), write separate tests for each.
- Never use `Thread.sleep` directly. Use `assertBusy` or `waitUntil` for polling, or instrument code with concurrency primitives like `CountDownLatch` for deterministic waiting.
- Do not depend on specific segment topology. If needed, disable background refreshes and force merge after indexing.
- Clean up all resources at the end of each test. The base test class checks for open file handles and running threads after tear down.
- Do not abuse randomization in multi-threaded tests because it can make failures non-reproducible.
- When testing functionality behind a feature flag, use `FeatureFlags.TestUtils` and the `@LockFeatureFlag` annotation.
- Use the `-Dtests.iters=N` parameter to repeat new and modified tests with many different random seeds to ensure stability.

## Java Formatting

- Formatted with Eclipse JDT formatter via Spotless Gradle plugin.
- Run `./gradlew spotlessJavaCheck` to check, `./gradlew spotlessApply` to fix.
- 4-space indent, 140-character line width.
- Wildcard imports are forbidden.
- Prefer `foo == false` over `!foo` for readability.

## Adding Dependencies

When adding or removing a dependency in any `build.gradle` (non-test scope):
1. Copy the library's `LICENSE.txt` and `NOTICE.txt` to `<component>/licenses/<artifact>-LICENSE.txt` and `<artifact>-NOTICE.txt`.
2. Run `./gradlew :<component>:updateSHAs` to generate the SHA file.
3. Verify with `./gradlew :<component>:check`.

## Backwards Compatibility

- Use `Version.onOrAfter` / `Version.before` checks when changing on-disk formats or encodings.
- Mark public API classes with `@PublicApi` (backwards compatibility guaranteed), `@InternalApi` (no guarantees), `@ExperimentalApi` (may change any time), or `@DeprecatedApi`.
- User-facing API changes require the `>breaking` PR label, a CHANGELOG entry, and deprecation log messages via `DeprecationLogger`.
- Run `./gradlew japicmp` to check API compatibility against the latest release.

## Commits

Ensure `./gradlew precommit` passes before creating a commit. Write commit message titles focused on user impact and not implementation details.

- Good: `Allow slash in snapshot file name validation`
- Bad: `Add Strings#validFileNameExcludingSlash method`

Commit message titles should be limited to 50 characters, followed by a blank line, and the body should be wrapped at 72 characters. Include all relevant context in commit messages but avoid excess verbosity. Users must understand and accept the Developer Certificate of Origin (DCO) and all commits must be signed off accordingly.

## Pull Requests

Always push to the user's fork. Never push to the upstream `opensearch-project/OpenSearch` repo. Never push directly to `main`. If a user fork does not exist, ask the contributor to create one.
