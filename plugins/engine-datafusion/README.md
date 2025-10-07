
## Prerequisites

1. Publish OpenSearch to maven local
```
./gradlew publishToMavenLocal
```
2. Publish SQL plugin to maven local
```
./gradlew publishToMavenLocal
```
3. Run opensearch with following parameters
```
 ./gradlew run --preserve-data -PremotePlugins="['org.opensearch.plugin:opensearch-job-scheduler:3.3.0.0-SNAPSHOT', 'org.opensearch.plugin:opensearch-sql-plugin:3.3.0.0-SNAPSHOT']" -PinstalledPlugins="['engine-datafusion']" --debug-jvm
```


## Steps to test indexing + search e2e

TODO : need to remove hardcoded index name `index-7`

1. Delete previous index if any
```
curl --location --request DELETE 'localhost:9200/index-7'
```

2. Create index with name : `index-7`
```
curl --location --request PUT 'http://localhost:9200/index-7' \
--header 'Content-Type: application/json' \
--data-raw '{
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": -1
    },
    "mappings": {
        "properties": {
            "message": {
                "type": "long"
            },
            "message2": {
                "type": "long"
            },
            "message3": {
                "type": "long"
            }
        }
    }
}'
```
3. Index docs
```
curl --location --request POST 'http://localhost:9200/_bulk' \
--header 'Content-Type: application/json' \
--data-raw '{"index":{"_index":"index-7"}}
{"message": 2,"message2": 3,"message3": 4}
{"index":{"_index":"index-7"}}
{"message": 3,"message2": 4,"message3": 5}
'
```
4. Refresh the index
```
curl localhost:9200/index-7/_refresh
```
5. Query
```
curl --location --request POST 'http://localhost:9200/_plugins/_ppl' \
--header 'Content-Type: application/json' \
--data-raw '{
  "query": "source=index-7 | stats count(), min(message) as min, max(message2) as max"
}'
```
