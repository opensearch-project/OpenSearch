# transport-grpc

An auxiliary transport which runs in parallel to the REST API.
The `transport-grpc` plugin initializes a new client/server transport implementing a gRPC protocol on Netty4.

Enable this transport with:

```
setting 'aux.transport.types', '[experimental-transport-grpc]'
setting 'aux.transport.experimental-transport-grpc.port', '9400-9500' //optional
```

## Testing

### Unit Tests

```
./gradlew :plugins:transport-grpc:test
```

### Integration Tests

```
./gradlew :plugins:transport-grpc:internalClusterTest
```
