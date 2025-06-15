# transport-grpc

An auxiliary transport which runs in parallel to the REST API.
The `transport-grpc` plugin initializes a new client/server transport implementing a gRPC protocol on Netty4.
## GRPC Settings
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


### Other gRPC Settings

| Setting Name                                    | Description                                                                                                    | Example Value         | Default Value        |
|-------------------------------------------------|----------------------------------------------------------------------------------------------------------------|-----------------------|----------------------|
| **grpc.publish_port**                           | The external port number that this node uses to publish itself to peers for gRPC transport.                    | `9400`                | `-1` (disabled)      |
| **grpc.host**                                   | List of addresses the gRPC server will bind to.                                                                | `["0.0.0.0"]`         | `[]`                 |
| **grpc.bind_host**                              | List of addresses to bind the gRPC server to. Can be distinct from publish hosts.                              | `["0.0.0.0", "::"]`   | Value of `grpc.host` |
| **grpc.publish_host**                           | List of hostnames or IPs published to peers for client connections.                                            | `["thisnode.example.com"]` | Value of `grpc.host` |
| **grpc.netty.worker_count**                     | Number of Netty worker threads for the gRPC server. Controls concurrency and parallelism.                      | `2`                   | Number of processors |
| **grpc.netty.max_concurrent_connection_calls**  | Maximum number of simultaneous in-flight requests allowed per client connection.                               | `200`                 | `100`                |
| **grpc.netty.max_connection_age**               | Maximum age a connection is allowed before being gracefully closed. Supports time units like `ms`, `s`, `m`.   | `500ms`               | Not set (no limit)   |
| **grpc.netty.max_connection_idle**              | Maximum duration a connection can be idle before being closed. Supports time units like `ms`, `s`, `m`.        | `2m`                  | Not set (no limit)   |
| **grpc.netty.keepalive_timeout**                | Time to wait for keepalive ping acknowledgment before closing the connection. Supports time units.             | `1s`                  | Not set              |
| **grpc.netty.max_msg_size**                     | Maximum inbound message size for gRPC requests. Supports units like `b`, `kb`, `mb`, `gb`.                    | `10mb` or `10485760`  | `10mb`               |

---

### Notes:
- For duration-based settings (e.g., `max_connection_age`), you can use units such as `ms` (milliseconds), `s` (seconds), `m` (minutes), etc.
- For size-based settings (e.g., `max_msg_size`), you can use units such as `b` (bytes), `kb`, `mb`, `gb`, etc.
- All settings are node-scoped unless otherwise specified.

### Example configurations:
```
setting 'grpc.publish_port',                            '9400'
setting 'grpc.host',                                    '["0.0.0.0"]'
setting 'grpc.bind_host',                               '["0.0.0.0", "::", "10.0.0.1"]'
setting 'grpc.publish_host',                            '["thisnode.example.com"]'
setting 'grpc.netty.worker_count',                      '2'
setting 'grpc.netty.max_concurrent_connection_calls',   '200'
setting 'grpc.netty.max_connection_age',                '500ms'
setting 'grpc.netty.max_connection_idle',               '2m'
setting 'grpc.netty.max_msg_size:                       '10mb'
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
