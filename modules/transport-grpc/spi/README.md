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

Registry that manages and discovers all available query converters. External plugins can register their custom converters through OpenSearch's ExtensiblePlugin mechanism.


## How It Works

The transport-grpc SPI uses OpenSearch's **ExtensiblePlugin mechanism**, which internally uses Java's **ServiceLoader**:

1. **Plugin Declaration**: The external plugin declares `extended.plugins=transport-grpc` in its descriptor
2. **SPI Registration**: The converter is listed in `META-INF/services/...QueryBuilderProtoConverter`
3. **Component Creation**: Your plugin returns the converter from `createComponents()`
4. **Discovery**: OpenSearch's `ExtensiblePlugin` mechanism discovers the external converter via ServiceLoader
5. **Registration**: The transport-grpc module automatically registers your converter

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

**Step 3a: Return Converter from createComponents()**

In your plugin's main class, create and return the converter:

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

        // Create your converter
        MyCustomQueryConverter converter = new MyCustomQueryConverter();

        // Return it - OpenSearch will automatically register it with gRPC transport
        return List.of(converter);
    }
}
```

**Step 3b: Create SPI Registration File**

Create a file at `src/main/resources/META-INF/services/org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverter`:

```
org.opensearch.mypackage.MyCustomQueryConverter
```

**Step 3c: Declare Extension in Plugin Descriptor**

In your `plugin-descriptor.properties`, declare that your plugin extends transport-grpc:

```properties
extended.plugins=transport-grpc
```

### 4. Accessing the Registry (For Complex Queries)

If your converter needs to handle nested queries (like k-NN's filter clause), you'll need access to the registry to convert other query types:

```java
public class MyCustomQueryConverter implements QueryBuilderProtoConverter {

    // Create your own registry instance to access other converters
    private static final QueryBuilderProtoConverterSpiRegistry registry =
        new QueryBuilderProtoConverterSpiRegistry();

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        MyCustomQuery customQuery = queryContainer.getMyCustomQuery();

        MyCustomQueryBuilder builder = new MyCustomQueryBuilder(
            customQuery.getField(),
            customQuery.getValue()
        );

        // Handle nested queries using the registry
        if (customQuery.hasFilter()) {
            QueryContainer filterContainer = customQuery.getFilter();
            QueryBuilder filterQuery = registry.fromProto(filterContainer);
            builder.filter(filterQuery);
        }

        return builder;
    }
}
```

**Why a separate registry instance is needed**
- `createComponents()` is called before dependency injection is available (line 1067 in Node.java)
- The main injector is created later (line 1680 in Node.java)
- Each plugin that needs to convert nested queries creates its own registry instance
- The registry automatically includes all built-in converters (MatchAll, Term, Terms, etc.) plus any discovered external converters


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

    // Create own registry instance to access other converters
    private static QueryBuilderProtoConverterSpiRegistry REGISTRY =
        new QueryBuilderProtoConverterSpiRegistry();

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

        // Handle nested filter query using registry
        if (knnQuery.hasFilter()) {
            QueryContainer filterContainer = knnQuery.getFilter();
            QueryBuilder filterQuery = REGISTRY.fromProto(filterContainer);
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
# src/main/resources/META-INF/services/org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverter
org.opensearch.knn.grpc.proto.request.search.query.KNNQueryBuilderProtoConverter
```

**Why k-NN needs the registry:**
The k-NN query's `filter` field is a `QueryContainer` protobuf type that can contain any query type (MatchAll, Term, Terms, etc.). The k-NN converter needs access to the registry to convert these nested queries to their corresponding QueryBuilder objects.
