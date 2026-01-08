# OpenSearch gRPC Transport Test Framework

The gRPC module test framework provides high-level abstractions for implementing integration tests for plugins which extend and utilize the gRPC transport. It solves the problem of breaking changes in protobuf schemas by providing stable APIs that abstract the underlying protobuf implementation details. When protobuf schemas change in the core gRPC transport module (e.g., protobuf field `SearchRequestBody` is renamed to `SearchRequest`), plugin tests that directly reference these protobuf classes break. This creates maintenance overhead and coupling between plugin tests and core implementation details.

## Usage

Include in plugin integration tests by adding the following dependency to build.gradle.
```gradle
dependencies {
    testImplementation project(':modules:transport-grpc:test-framework')
}
```

Example usage of `OpenSearchIntegTestCase` and helper functions.
```java
public class MyPluginTest extends OpenSearchIntegTestCase {

    public void testMyPluginFeature() {
        ManagedChannel plainTextGrpcChannel = NettyChannelBuilder.forAddress("localhost", 9200).
                proxyDetector(NOOP_PROXY_DETECTOR).
                usePlaintext()
                .build();

        // Bulk ingest test documents with gRPC
        doBulk(plainTextGrpcChannel, "test-index", 3);
    }
}
```
