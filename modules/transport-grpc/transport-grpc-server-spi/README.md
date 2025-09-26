# transport-grpc-server-spi

Service Provider Interface (SPI) for providing gRPC related interfaces to extending plugins.
Covers dependencies related `NettyServerBuilder` such as `ServerInterceptor` and `BindableService` implementatins.

## Key Components

### GrpcServiceFactory

Interface for providing a `BindableService` factory to be registered on the grpc transport.

Include this SPI in your plugin `build.gradle` with:

```gradle
dependencies {
    compileOnly 'org.opensearch.plugin:transport-grpc-server-spi:${opensearch.version}'
    implementation "io.grpc:grpc-api:${versions.grpc}"
    runtimeOnly "com.google.guava:guava:${versions.guava}"
    runtimeOnly "com.google.guava:failureaccess:1.0.2"
}
```

Add a service registration file to your plugin, such as:

Create a file at `src/main/resources/META-INF/services/org.opensearch.transport.grpc.server.spi.GrpcServiceFactory`:

with contents:

```
org.opensearch.transport.grpc.LoadExtendingPluginServicesIT$MockExtendingPlugin$MockServiceProvider
```

Provide the implementation for your `GrpcServiceFactory` within your plugin:

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
