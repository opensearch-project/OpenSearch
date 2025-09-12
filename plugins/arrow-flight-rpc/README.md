# Arrow Flight RPC Plugin

The Arrow Flight RPC plugin provides streaming transport for node to node communication in OpenSearch using Apache Arrow Flight protocol. It integrates with the OpenSearch Security plugin to provide secure, authenticated streaming with TLS encryption.

## Installation and Setup

### Development Mode (./gradlew run)

For development using gradle:

1. Enable feature flag in `opensearch.yml`:
```yaml
opensearch.experimental.feature.transport.stream.enabled: true
```

2. Run with plugin:
```bash
./gradlew run -PinstalledPlugins="['arrow-flight-rpc']"
```

### Manual Setup

For manual configuration and deployment:

1. Enable feature flag in `opensearch.yml`:
```yaml
opensearch.experimental.feature.transport.stream.enabled: true
```

2. Add system properties and JVM options:
```
-Dio.netty.allocator.numDirectArenas=1
-Dio.netty.noUnsafe=false
-Dio.netty.tryUnsafe=true
-Dio.netty.tryReflectionSetAccessible=true
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

3. Install and run the plugin manually

## Documentation

For detailed usage and architecture information, see the [docs](docs/) folder:

- [Architecture Guide](docs/architecture.md) - Stream transport architecture and design
- [Server-side Streaming Guide](docs/server-side-streaming-guide.md) - How to implement server-side streaming
- [Transport Client Streaming Flow](docs/transport-client-streaming-flow.md) - Client-side streaming implementation
- [Flight Client Channel Flow](docs/flight-client-channel-flow.md) - Client channel flow details
- [Metrics](docs/metrics.md) - Monitoring and performance metrics
- [Error Handling](docs/error-handling.md) - Error handling patterns
- [Security Integration](docs/security-integration.md) - Security plugin integration and TLS setup
- [Chaos Testing](docs/chaos.md) - Chaos testing setup and usage
- [Netty4 vs Flight Comparison](docs/netty4-vs-flight-comparison.md) - Transport classes comparison cheat sheet

## Examples

See the [stream-transport-example](../examples/stream-transport-example/) plugin for a complete example of how to implement streaming transport actions.

## Limitations

- **REST Client Support**: Arrow Flight streaming is not available for REST API clients. It only works for node-to-node transport within the OpenSearch cluster.
