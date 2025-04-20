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
setting 'aux.transport.types',                                      '[experimental-secure-transport-grpc]'
setting 'aux.transport.experimental-secure-transport-grpc.port',    '9400-9500' //optional
```

Other gRPC settings:

```
setting 'grpc.publish_port',                            '9400'
setting 'grpc.host',                                    '["0.0.0.0"]'
setting 'grpc.bind_host',                               '["0.0.0.0", "::", "10.0.0.1"]'
setting 'grpc.publish_host',                            '["thisnode.example.com"]'
setting 'grpc.netty.worker_count',                      '2'
setting 'grpc.netty.max_concurrent_connection_calls',   '200'
setting 'grpc.netty.max_connection_age',                '500ms'
setting 'grpc.netty.max_connection_idle',               '2m'
setting 'grpc.netty.keepalive_timeout',                 '1s'
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
