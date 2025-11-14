# transport-grpc-spi

Service Provider Interface (SPI) for the OpenSearch gRPC transport module. This module provides interfaces and utilities that allow external plugins to extend the gRPC transport functionality.

## Overview

The `transport-grpc-spi` module enables plugin developers to:
- Implement custom query converters for gRPC transport
- Extend gRPC protocol buffer handling
- Register custom query types that can be processed via gRPC
- Register gRPC interceptors with explicit ordering
- Register `BindableService` implementation to the gRPC transport

## Key Components

### QueryBuilderProtoConverter

Interface for converting protobuf query messages to OpenSearch QueryBuilder objects.

```java
public interface QueryBuilderProtoConverter {
    QueryContainer.QueryContainerCase getHandledQueryCase();
    QueryBuilder fromProto(QueryContainer queryContainer);
}
```

### QueryBuilderProtoConverterRegistry

Interface for accessing the query converter registry. This provides a clean abstraction for plugins that need to convert nested queries without exposing internal implementation details.

### GrpcInterceptorProvider

Interface for providing gRPC interceptors that can intercept and process all incoming gRPC requests. This enables plugins to implement cross-cutting concerns like authentication, authorization, logging, and metrics collection.

```java
public interface GrpcInterceptorProvider {
    List<OrderedGrpcInterceptor> getOrderedGrpcInterceptors();

    /**
     * Nested interface for ordered gRPC interceptors.
     * Interceptors are executed in order based on their order() value,
     * with lower values executing first (e.g., 10 before 20).
     */
    interface OrderedGrpcInterceptor {
        int order();  // Lower values execute first
        ServerInterceptor getInterceptor();
    }
}
```

### GrpcServiceFactory

Interface for providing a `BindableService` factory to be registered on the grpc transport.

## Usage for Plugin Developers

### 1. Add Dependency

Add the SPI dependency to your plugin's `build.gradle`:

```gradle
dependencies {
    compileOnly 'org.opensearch.plugin:transport-grpc-spi:${opensearch.version}'
    compileOnly 'org.opensearch:protobufs:${protobufs.version}'
    compileOnly 'io.grpc:grpc-api:${versions.grpc}'
}
```

### 2. Declare Extension in build.gradle

In your `build.gradle`, declare that your plugin extends `transport-grpc`. This automatically adds the `extended.plugins=transport-grpc` entry to the auto-generated `plugin-descriptor.properties` file:

```groovy
opensearchplugin {
    name 'your-plugin-name'
    description 'Your plugin description'
    classname 'org.opensearch.yourplugin.YourPlugin'
    extendedPlugins = ['transport-grpc']  // Declare extension here
}
```
**Real-world examples:**
- [OpenSearch Reporting Plugin](https://github.com/opensearch-project/reporting/blob/main/build.gradle#L92)
- [OpenSearch k-NN Plugin](https://github.com/opensearch-project/k-NN/blob/main/build.gradle#L319)

### 3. Create SPI Registration File(s)

Create a service file denoting your plugin's implementation of a service interface.

For `QueryBuilderProtoConverter` implementations:
`src/main/resources/META-INF/services/org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter`:

```
org.opensearch.mypackage.MyCustomQueryConverter
```

For `GrpcInterceptorProvider` implementations:
`src/main/resources/META-INF/services/org.opensearch.transport.grpc.spi.GrpcInterceptorProvider`:

```
org.opensearch.mypackage.SampleInterceptorProvider
```

For `GrpcServiceFactory` implementations:
`src/main/resources/META-INF/services/org.opensearch.transport.grpc.spi.GrpcServiceFactory`:

```
org.opensearch.mypackage.MyCustomGrpcServiceFactory
```

## QueryBuilderProtoConverter
### 1. Implement Custom Query Converter

```java
public class MyCustomQueryConverter implements QueryBuilderProtoConverter {

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.MY_CUSTOM_QUERY;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        // Convert your custom protobuf query to QueryBuilder
        MyCustomQuery customQuery = queryContainer.getMyCustomQuery();
        return new MyCustomQueryBuilder(customQuery.getField(), customQuery.getValue());
    }
}
```

### 2. Register Your Converter

In your plugin's main class, return the converter from createComponents:

```java
public class MyPlugin extends Plugin {

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService,
                                             ThreadPool threadPool, ResourceWatcherService resourceWatcherService,
                                             ScriptService scriptService, NamedXContentRegistry xContentRegistry,
                                             Environment environment, NodeEnvironment nodeEnvironment,
                                             NamedWriteableRegistry namedWriteableRegistry,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<RepositoriesService> repositoriesServiceSupplier) {

        // Return your converter instance - the transport-grpc plugin will discover and register it
        return Collections.singletonList(new MyCustomQueryConverter());
    }
}
```

### 3. Accessing the Registry (For Complex Queries)

If your converter needs to handle nested queries (like k-NN's filter clause), you'll need access to the registry to convert other query types. The transport-grpc plugin will inject the registry into your converter.

```java
public class MyCustomQueryConverter implements QueryBuilderProtoConverter {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        MyCustomQuery customQuery = queryContainer.getMyCustomQuery();

        MyCustomQueryBuilder builder = new MyCustomQueryBuilder(
            customQuery.getField(),
            customQuery.getValue()
        );

        // Handle nested queries using the injected registry
        if (customQuery.hasFilter()) {
            QueryContainer filterContainer = customQuery.getFilter();
            QueryBuilder filterQuery = registry.fromProto(filterContainer);
            builder.filter(filterQuery);
        }

        return builder;
    }
}
```

**Registry Injection Pattern**

**How k-NN Now Accesses Built-in Converters**:

The gRPC plugin **injects the populated registry** into converters that need it:

```java
// 1. Converter interface has a default setRegistry method
public interface QueryBuilderProtoConverter {
    QueryBuilder fromProto(QueryContainer queryContainer);

    default void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        // By default, converters don't need a registry
        // Converters that handle nested queries should override this method
    }
}

// 2. GrpcPlugin injects registry into loaded extensions
for (QueryBuilderProtoConverter converter : queryConverters) {
    // Inject the populated registry into the converter
    converter.setRegistry(queryRegistry);

    // Register the converter
    queryRegistry.registerConverter(converter);
}
```

**Registry Access Pattern for Converters with Nested Queries**:
```java
public class KNNQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        this.registry = registry;
        // Pass the registry to utility classes that need it
        KNNQueryBuilderProtoUtils.setRegistry(registry);
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        // The utility class can now convert nested queries using the injected registry
        return KNNQueryBuilderProtoUtils.fromProto(queryContainer.getKnn());
    }
}
```


## Testing

### Unit Tests

```bash
./gradlew :modules:transport-grpc:spi:test
```

### 4. Testing Your Custom Converter

```java
@Test
public void testCustomQueryConverter() {
    MyCustomQueryConverter converter = new MyCustomQueryConverter();

    // Create test protobuf query
    QueryContainer queryContainer = QueryContainer.newBuilder()
        .setMyCustomQuery(MyCustomQuery.newBuilder()
            .setField("test_field")
            .setValue("test_value")
            .build())
        .build();

    // Convert and verify
    QueryBuilder result = converter.fromProto(queryContainer);
    assertThat(result, instanceOf(MyCustomQueryBuilder.class));

    MyCustomQueryBuilder customQuery = (MyCustomQueryBuilder) result;
    assertEquals("test_field", customQuery.fieldName());
    assertEquals("test_value", customQuery.value());
}
```

## Real-World Example: k-NN Plugin
See the k-NN plugin https://github.com/opensearch-project/k-NN/pull/2833/files for an example on how to use this SPI, including handling nested queries.

**1. Dependency in build.gradle:**
```gradle
compileOnly "org.opensearch.plugin:transport-grpc-spi:${opensearch.version}"
compileOnly "org.opensearch:protobufs:0.8.0"
```

**2. Converter Implementation with Registry Access:**
```java
public class KNNQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.KNN;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        KnnQuery knnQuery = queryContainer.getKnn();

        KNNQueryBuilder builder = new KNNQueryBuilder(
            knnQuery.getField(),
            knnQuery.getVectorList().toArray(new Float[0]),
            knnQuery.getK()
        );

        // Handle nested filter query using injected registry
        if (knnQuery.hasFilter()) {
            QueryContainer filterContainer = knnQuery.getFilter();
            QueryBuilder filterQuery = registry.fromProto(filterContainer);
            builder.filter(filterQuery);
        }

        return builder;
    }
}
```

**3. Plugin Registration:**
```java
// In KNNPlugin.createComponents()
KNNQueryBuilderProtoConverter knnQueryConverter = new KNNQueryBuilderProtoConverter();
return ImmutableList.of(knnStats, knnQueryConverter);
```

**4. SPI File:**
```
# src/main/resources/META-INF/services/org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter
org.opensearch.knn.grpc.proto.request.search.query.KNNQueryBuilderProtoConverter
```

**Why k-NN needs the registry:**
The k-NN query's `filter` field is a `QueryContainer` protobuf type that can contain any query type (MatchAll, Term, Terms, etc.). The k-NN converter needs access to the registry to convert these nested queries to their corresponding QueryBuilder objects.


## gRPC Interceptors

### Overview

Intercept incoming gRPC requests for authentication, authorization, logging, metrics, rate limiting, etc. Interceptors have access to OpenSearch's `ThreadContext` to store and retrieve request-scoped data.

**Context Preservation:** The transport-grpc module automatically preserves ThreadContext across async boundaries. Any data set by interceptors will be available in the gRPC service implementation, even when execution switches to different threads.

### Basic Usage

**1. Implement Provider:**
```java
public class SampleInterceptorProvider implements GrpcInterceptorProvider {
    @Override
    public List<GrpcInterceptorProvider.OrderedGrpcInterceptor> getOrderedGrpcInterceptors(ThreadContext threadContext) {
        return Arrays.asList(
            // First interceptor (order = 5, runs first)
            new GrpcInterceptorProvider.OrderedGrpcInterceptor() {
                @Override
                public int order() { return 5; } // Lower = higher priority

                @Override
                public ServerInterceptor getInterceptor() {
                    return (call, headers, next) -> {
                        String methodName = call.getMethodDescriptor().getFullMethodName();
                        threadContext.putTransient("grpc.method", methodName);
                        System.out.println("First interceptor - Method: " + methodName);
                        return next.startCall(call, headers);
                    };
                }
            },

            // Second interceptor (order = 10, runs after first)
            new GrpcInterceptorProvider.OrderedGrpcInterceptor() {
                @Override
                public int order() { return 10; }

                @Override
                public ServerInterceptor getInterceptor() {
                    return (call, headers, next) -> {
                        System.out.println("Second interceptor - Processing request");
                        return next.startCall(call, headers);
                    };
                }
            }
        );
    }
}
```

### Understanding Interceptor Ordering

#### How Order Values Work

Interceptors are executed based on their `order()` value. Lower order values execute first. The interceptor chain processes requests from lowest to highest order value.

```
Request → [order=5] → [order=10] → [order=100] → Service Handler
```

#### Duplicate Order Values

Each interceptor must have a unique order value. If duplicate order values are detected, OpenSearch will fail to start with an `IllegalArgumentException`.
```
IllegalArgumentException: Multiple gRPC interceptors have the same order value: 10.
Each interceptor must have a unique order value.
```

## GrpcServiceFactory

### 1. Implement Custom Query Converter

Several node resources are exposed to a `GrpcServiceFactory` for use within services such as client, settings, and thread pools.
A plugin's `GrpcServiceFactory` implementation will be discovered through the SPI registration file and registered on the gRPC transport.

```java
public static class MockServiceProvider implements GrpcServiceFactory {

    @Override
    public String plugin() {
        return "MockExtendingPlugin";
    }

    @Override
    public List<BindableService> build() {
        return List.of(new MockChannelzService());
    }
}
```
