## OpenSearch API Plugin

A plugin that returns the OpenSearch API in the OpenAPI format.

### Build and Test

```sh
./gradlew :plugins:api:build
```

You can run various test suites individually.

```sh
./gradlew :plugins:api:test # unit tests
./gradlew :plugins:api:integTest # integration tests
./gradlew :plugins:api:yamlRestTest # YAML REST tests
```

### Run

When you run OpenSearch with `./gradlew run -PinstalledPlugins="['api']"` you should see the plugin loaded in the logs.

```
[2023-09-27T08:35:48,627][INFO ][o.o.p.PluginsService] [runTask-0] loaded plugin [api]
```

Alternately you can modify `gradle/run.gradle`.

```gradle
testClusters {
  runTask {
    plugin(':plugins:api')
  }
}
```

### Try

```sh
$ curl http://localhost:9200/_plugins/api
```

Returns an OpenAPI spec.

```json
{
  "openapi": "3.0.1",
  "info": {
    "title": "opensearch",
    "description": "The OpenSearch Project: https://opensearch.org/",
    "version": "3.0.0-SNAPSHOT"
  },
  "paths": {
    "/_nodes": {
      "get": {}
    },
    "/_cluster/state/:metric": {
      "get": {}
    },
    ...
  }
```
