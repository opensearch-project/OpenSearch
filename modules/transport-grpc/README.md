# transport-grpc

An auxiliary transport which runs in parallel to the REST API.
The `transport-grpc` module initializes a new client/server transport implementing a gRPC protocol on Netty4.

**Note:** As a module, transport-grpc is included by default with all OpenSearch installations. However, it remains opt-in and must be explicitly enabled via configuration settings.

## Table of Contents

- [transport-grpc](#transport-grpc)
  - [Table of Contents](#table-of-contents)
  - [Contributing to gRPC APIs](#contributing-to-grpc-apis)
    - [Overview](#overview)
    - [Workflow](#workflow)
    - [Step-by-Step Guide](#step-by-step-guide)
      - [Step 0: Submit Fixes to Related Repositories (If Needed)](#step-0-submit-fixes-to-related-repositories-if-needed)
      - [Step 1: Generate Protobuf Definitions from OpenAPI Spec](#step-1-generate-protobuf-definitions-from-openapi-spec)
      - [Step 2: Generate and Package Local Protobuf JAR](#step-2-generate-and-package-local-protobuf-jar)
      - [Step 3: Set Up OpenSearch Repository with Local Protobuf JAR](#step-3-set-up-opensearch-repository-with-local-protobuf-jar)
      - [Step 4: Implement Your Core Contribution](#step-4-implement-your-core-contribution)
        - [A. Adding a Core Query Converter](#a-adding-a-core-query-converter)
        - [B. Extending a gRPC Service](#b-extending-a-grpc-service)
        - [C. Building a Custom Core Interceptor](#c-building-a-custom-core-interceptor)
      - [Step 5: Test Your Implementation](#step-5-test-your-implementation)
      - [Step 6: Submit Pull Requests](#step-6-submit-pull-requests)
    - [Merge Order Summary](#merge-order-summary)
  - [Development Guide](#development-guide)
    - [GRPC Settings](#grpc-settings)
      - [Other gRPC Settings](#other-grpc-settings)
      - [Notes:](#notes)
      - [Example configurations:](#example-configurations)
    - [Thread Pool Monitoring](#thread-pool-monitoring)
    - [Testing](#testing)
      - [Unit Tests](#unit-tests)
      - [Integration Tests](#integration-tests)
      - [Running OpenSearch with gRPC Enabled](#running-opensearch-with-grpc-enabled)

## Contributing to gRPC APIs

This section is for contributors who want to add new gRPC functionality directly to OpenSearch core (not as an external plugin). This includes adding new query converters, extending gRPC services, or building custom interceptors that will be part of the core distribution.

### Overview

Contributing to core gRPC may involve working with up to four repositories:
1. **opensearch-api-specification**: Fix API spec errors (if any are found)
2. **opensearch-protobufs**: Generate protobuf schemas from the API spec
3. **OpenSearch**: Implement Java code that uses the protobufs (query converters, services, interceptors)
4. **documentation-website**: Update public documentation for your gRPC APIs

**Note:** If you're only building interceptors (without protobuf changes), you can skip Steps 1-2 and start directly at Step 3.

### Workflow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. opensearch-protobufs: Define/modify .proto files        │
│    - Add new message types                                  │
│    - Add new service endpoints                              │
│    - Generate local protobuf JAR                            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. OpenSearch: Implement Java code                         │
│    - Import local protobuf JAR (if protos changed)         │
│    - Implement query converters / services / interceptors   │
│    - Test your implementation                               │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. Submit PRs (coordinated merge)                           │
│    - PR to opensearch-protobufs (merged first)             │
│    - PR to OpenSearch (update version, then merge)         │
│    - PR to documentation-website (can merge independently)  │
└─────────────────────────────────────────────────────────────┘
```

### Step-by-Step Guide

#### Step 0: Submit Fixes to Related Repositories (If Needed)

**Before generating protobufs**, if you discover errors while working with the API:

- **API Specification fixes:** If you find errors in the OpenSearch API spec, submit a PR to [opensearch-api-specification](https://github.com/opensearch-project/opensearch-api-specification). After the spec fix is merged, regenerate protobufs to incorporate the corrections.
- **Documentation fixes:** If you find errors in public documentation, submit a PR to [documentation-website](https://github.com/opensearch-project/documentation-website)
- **Remember:** The server code (`toXContent`/`fromXContent` methods for REST APIs) is the source of truth. If there's a discrepancy between the API spec and server code, the server is correct.

#### Step 1: Generate Protobuf Definitions from OpenAPI Spec

**Skip this step if you're only building interceptors without protobuf changes.**

Clone the opensearch-protobufs repository and follow the [Protobuf Local Convert Guide](https://github.com/opensearch-project/opensearch-protobufs/blob/main/DEVELOPER_GUIDE.md#protobuf-local-convert-guide) to generate protobufs from the OpenSearch API specification.

```bash
git clone https://github.com/opensearch-project/opensearch-protobufs.git
cd opensearch-protobufs
```

#### Step 2: Generate and Package Local Protobuf JAR

**Skip this step if you're only building interceptors without protobuf changes.**

**Follow the complete guide:** [Generate Java Code and Packaging as a Maven/Gradle Dependency](https://github.com/opensearch-project/opensearch-protobufs/blob/main/DEVELOPER_GUIDE.md#generate-java-code-and-packaging-as-a-mavengradle-dependency)

**Summary:**
1. Build the Java protobuf code with Bazel
2. Run `./tools/java/package_proto_jar.sh` to create and install the JAR to your local Maven repository
3. Note the version from `version.properties` (e.g., `1.1.0-SNAPSHOT`)

The JAR will be installed to `~/.m2/repository/org/opensearch/protobufs/opensearch-protobufs/`

#### Step 3: Set Up OpenSearch Repository with Local Protobuf JAR

1. **Clone OpenSearch repository (if not already cloned):**
   ```bash
   git clone https://github.com/opensearch-project/OpenSearch.git
   cd OpenSearch
   ```

2. **Configure to use local protobuf JAR (if you made protobuf changes):**

   Edit `build.gradle` in the OpenSearch root:
   ```groovy
   allprojects {
       repositories {
           mavenLocal()  // Add this to use your local Maven repository
           // ... other repositories ...
       }
   }
   ```

   Then update the version in `gradle/libs.versions.toml`:
   ```toml
   opensearchprotobufs = "1.1.0-SNAPSHOT"
   ```

3. **Verify the setup:**
   ```bash
   ./gradlew :modules:transport-grpc:build
   ```

#### Step 4: Implement Your Core Contribution

Now you can implement your gRPC functionality in the OpenSearch codebase. Choose the appropriate section based on what you're building.

##### A. Adding a Core Query Converter

**Requires protobuf changes (Steps 1-2).**

Query converters transform protobuf query messages into OpenSearch QueryBuilder objects.

**Implementation details:** See [SPI documentation](spi/README.md#querybuilderprotoconverter) for complete examples.

##### B. Extending a gRPC Service

**Requires protobuf changes (Steps 1-2).**

gRPC services expose new API endpoints for client applications.

**Implementation details:** See [SPI documentation](spi/README.md#grpcservicefactory) for complete examples.

##### C. Building a Custom Core Interceptor

**Does NOT require protobuf changes - you can skip Steps 1-2 and start here!**

Interceptors process all incoming gRPC requests for cross-cutting concerns like authentication, logging, and metrics.

**Implementation details:** See [SPI documentation](spi/README.md#grpc-interceptors) for complete examples.

#### Step 5: Test Your Implementation

See the [Testing](#testing) section in the Development Guide below for complete instructions on:
- Running unit tests
- Running integration tests
- Manual testing with OpenSearch

#### Step 6: Submit Pull Requests

**Important: PRs must be coordinated and merged in a specific order.**

1. **Submit PR to opensearch-protobufs (if you made protobuf changes):**
   - Title: "Add protobuf definitions for [feature name]"
   - Include:
     - New/modified `.proto` files
     - Updated documentation
     - Reasoning for the changes
   - Link to the corresponding OpenSearch PR
   - **This PR must be merged FIRST**

2. **After protobufs PR is merged, update OpenSearch:**
   - Update the protobuf version in `gradle/libs.versions.toml`:
     ```toml
     opensearchprotobufs = "1.1.0"  # Use the newly published version
     ```
   - Remove `mavenLocal()` from `build.gradle` if you added it

3. **Submit PR to OpenSearch:**
   - Title: "Add gRPC support for [feature name]"
   - Include:
     - Implementation code (converters/services/interceptors)
     - Unit/integration tests
     - Updated `gradle/libs.versions.toml` (if protobuf version changed)
   - Reference the opensearch-protobufs PR in the description
   - **This PR can be merged AFTER the protobufs PR is merged and the version is updated**

4. **Submit PR to documentation-website:**
   - Update the gRPC API documentation with your new features
   - Example: [PR #11614 - gRPC Documentation for 3.4](https://github.com/opensearch-project/documentation-website/pull/11614)
   - Include:
     - API reference updates
     - Usage examples
     - Any new configuration options
   - **This PR can be merged independently** but should ideally be coordinated with the OpenSearch release

### Merge Order Summary

```
0. (If needed) opensearch-api-specification PR → MERGE FIRST (if API spec fixes are required)
   ↓ Regenerate protobufs after spec fix is merged
   ↓
1. opensearch-protobufs PR → MERGE FIRST
   ↓
2. Protobufs published to Maven Central
   ↓
3. Update OpenSearch gradle/libs.versions.toml with new version
   ↓
4. OpenSearch PR → MERGE SECOND
   ↓
5. documentation-website PR → MERGE (can be independent)
```

---

## Development Guide

### GRPC Settings

Enable this transport with:

```
setting 'aux.transport.types',                              '[transport-grpc]'
setting 'aux.transport.transport-grpc.port',   '9400-9500' //optional
```

For the secure transport:

```
setting 'aux.transport.types',                                      '[secure-transport-grpc]'
setting 'aux.transport.secure-transport-grpc.port',    '9400-9500' //optional
```

#### Other gRPC Settings

| Setting Name                                    | Description                                                                                                    | Example Value         | Default Value        |
|-------------------------------------------------|----------------------------------------------------------------------------------------------------------------|-----------------------|----------------------|
| **grpc.publish_port**                           | The external port number that this node uses to publish itself to peers for gRPC transport.                    | `9400`                | `-1` (disabled)      |
| **grpc.host**                                   | List of addresses the gRPC server will bind to.                                                                | `["0.0.0.0"]`         | `[]`                 |
| **grpc.bind_host**                              | List of addresses to bind the gRPC server to. Can be distinct from publish hosts.                              | `["0.0.0.0", "::"]`   | Value of `grpc.host` |
| **grpc.publish_host**                           | List of hostnames or IPs published to peers for client connections.                                            | `["thisnode.example.com"]` | Value of `grpc.host` |
| **grpc.netty.worker_count**                     | Number of Netty worker threads for the gRPC server. Controls network I/O concurrency.                          | `2`                   | Number of processors |
| **grpc.netty.executor_count**                   | Number of threads in the ForkJoinPool for processing gRPC service calls. Controls request processing parallelism. | `32`                  | 2 × Number of processors |
| **grpc.netty.max_concurrent_connection_calls**  | Maximum number of simultaneous in-flight requests allowed per client connection.                               | `200`                 | `100`                |
| **grpc.netty.max_connection_age**               | Maximum age a connection is allowed before being gracefully closed. Supports time units like `ms`, `s`, `m`.   | `500ms`               | Not set (no limit)   |
| **grpc.netty.max_connection_idle**              | Maximum duration a connection can be idle before being closed. Supports time units like `ms`, `s`, `m`.        | `2m`                  | Not set (no limit)   |
| **grpc.netty.keepalive_timeout**                | Time to wait for keepalive ping acknowledgment before closing the connection. Supports time units.             | `1s`                  | Not set              |
| **grpc.netty.max_msg_size**                     | Maximum inbound message size for gRPC requests. Supports units like `b`, `kb`, `mb`, `gb`.                    | `10mb` or `10485760`  | `10mb`               |

---

#### Notes:
- For duration-based settings (e.g., `max_connection_age`), you can use units such as `ms` (milliseconds), `s` (seconds), `m` (minutes), etc.
- For size-based settings (e.g., `max_msg_size`), you can use units such as `b` (bytes), `kb`, `mb`, `gb`, etc.
- All settings are node-scoped unless otherwise specified.

#### Example configurations:
```
setting 'grpc.publish_port',                            '9400'
setting 'grpc.host',                                    '["0.0.0.0"]'
setting 'grpc.bind_host',                               '["0.0.0.0", "::", "10.0.0.1"]'
setting 'grpc.publish_host',                            '["thisnode.example.com"]'
setting 'grpc.netty.worker_count',                      '2'
setting 'grpc.netty.executor_count',                    '32'
setting 'grpc.netty.max_concurrent_connection_calls',   '200'
setting 'grpc.netty.max_connection_age',                '500ms'
setting 'grpc.netty.max_connection_idle',               '2m'
setting 'grpc.netty.max_msg_size',                      '10mb'
setting 'grpc.netty.keepalive_timeout',                 '1s'
```

### Thread Pool Monitoring

The dedicated thread pool used for gRPC request processing is registered as a standard OpenSearch thread pool named `grpc`, controlled by the `grpc.netty.executor_count` setting.

The gRPC thread pool stats can be monitored using:

```bash
curl -X GET "localhost:9200/_nodes/stats/thread_pool?filter_path=nodes.*.thread_pool.grpc"
```

### Testing

#### Unit Tests

```bash
./gradlew :modules:transport-grpc:test
```

#### Integration Tests

```bash
./gradlew :modules:transport-grpc:internalClusterTest
```

#### Running OpenSearch with gRPC Enabled

To run OpenSearch with the gRPC transport enabled:

```bash
./gradlew run -Dtests.opensearch.aux.transport.types="[transport-grpc]"
```
