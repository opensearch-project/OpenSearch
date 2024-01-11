## OpenSearch API Plugin

A plugin that returns the OpenSearch API in the OpenAPI format.

### Build and Test

```sh
./gradlew :plugins:api:test # unit tests
./gradlew :plugins:api:integTest # integration tests
./gradlew :plugins:api:yamlRestTest # YAML REST tests
```

### Run

Modify `gradle/run.gradle`.

```gradle
testClusters {
  runTask {
    testDistribution = 'archive'
    plugin(':plugins:api')
  }
}
```

    When you run OpenSearch with `./gradlew run` you should see the plugin loaded in the logs.

```
[2023-09-27T08:35:48,627][INFO ][o.o.p.PluginsService] [runTask-0] loaded plugin [api]
```

### Test

```sh
$ curl http://localhost:9200/_plugins/api
```
