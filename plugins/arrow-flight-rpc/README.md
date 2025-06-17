# arrow-flight-rpc

Enable this transport with:

```
setting 'aux.transport.types',                   '[arrow-flight-rpc]'
setting 'aux.transport.arrow-flight-rpc.port',   '9400-9500' //optional
```

## Testing

### Unit Tests

```
./gradlew run \
    -PinstalledPlugins="['arrow-flight-rpc']" \
    -Dtests.opensearch.aux.transport.types="[experimental-transport-arrow-flight-rpc]" \
    -Dtests.opensearch.opensearch.experimental.feature.arrow.streams.enabled=true
```

### Unit Tests

```
./gradlew :plugins:arrow-flight-rpc:test
```

### Integration Tests

```
./gradlew :plugins:arrow-flight-rpc:internalClusterTest
```
