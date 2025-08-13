# transport-grpc-spi

Service Provider Interface (SPI) for the OpenSearch gRPC transport module. This module provides interfaces and utilities that allow external plugins to extend the gRPC transport functionality.

## Overview

The `transport-grpc-spi` module enables plugin developers to:
- Implement custom query converters for gRPC transport
- Extend gRPC protocol buffer handling
- Register custom query types that can be processed via gRPC

## Key Components

### QueryBuilderProtoConverter

Interface for converting protobuf query messages to OpenSearch QueryBuilder objects.

```java
public interface QueryBuilderProtoConverter {
    QueryContainer.QueryContainerCase getHandledQueryCase();
    QueryBuilder fromProto(QueryContainer queryContainer);
}
```

### QueryBuilderProtoConverterSpiRegistry

Registry that manages and discovers all available query converters. External plugins can register their custom converters through this registry.

## Usage for Plugin Developers

### 1. Add Dependency

Add the SPI dependency to your plugin's `build.gradle`:

```gradle
dependencies {
    compileOnly 'org.opensearch.plugin:transport-grpc-spi:${opensearch.version}'
    compileOnly 'org.opensearch:protobufs:${protobufs.version}'
}
```

### 2. Implement Custom Query Converter

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

### 3. Register Your Converter

In your plugin's main class, register the converter:

```java
public class MyPlugin extends Plugin implements ExtensiblePlugin {

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService,
                                             ThreadPool threadPool, ResourceWatcherService resourceWatcherService,
                                             ScriptService scriptService, NamedXContentRegistry xContentRegistry,
                                             Environment environment, NodeEnvironment nodeEnvironment,
                                             NamedWriteableRegistry namedWriteableRegistry,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<RepositoriesService> repositoriesServiceSupplier) {

        // Get the registry and register your converter
        QueryBuilderProtoConverterSpiRegistry registry =
            // Obtain registry from OpenSearch's dependency injection
        registry.registerConverter(new MyCustomQueryConverter());

        return Collections.emptyList();
    }
}
```

## Testing

### Unit Tests

```bash
./gradlew :modules:transport-grpc:spi:test
```

### Testing Your Custom Converter

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
