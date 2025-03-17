# transport-grpc

An auxiliary transport which runs in parallel to the REST API.
The `transport-grpc` plugin initializes a new client/server transport implementing a gRPC protocol on Netty4.

Enable this transport with:

```
setting 'aux.transport.types',                              '[experimental-transport-grpc]'
setting 'aux.transport.experimental-transport-grpc.port',   '9400-9500' //optional
```

For the secure transport:

```
setting 'aux.transport.types',                              '[experimental-transport-grpc]'
setting 'aux.transport.experimental-transport-grpc.port',   '9400-9500' //optional
```

Other settings are agnostic as to the gRPC transport type:

```
setting 'grpc.publish_port',        '9400'
setting 'grpc.host',                '["0.0.0.0"]'
setting 'grpc.bind_host',           '["0.0.0.0", "::", "10.0.0.1"]'
setting 'grpc.publish_host',        '["thisnode.example.com"]'
setting 'grpc.netty.worker_count',  '2'
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
