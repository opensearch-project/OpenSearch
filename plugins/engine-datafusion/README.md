
## Prerequisites

1. Checkout branch `substrait-plan` for OpenSearch SQL Plugin - https://github.com/vinaykpud/sql/tree/substrait-plan OR  https://github.com/bharath-techie/sql/tree/substrait-plan

2. Publish OpenSearch to maven local
```
./gradlew publishToMavenLocal
```
3. Publish SQL plugin to maven local
```
./gradlew publishToMavenLocal
```
4. Run opensearch with following parameters
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
            "id": {
                "type": "keyword"
            },
            "name": {
                "type": "keyword"
            },
            "age": {
                "type": "integer"
            },
            "salary": {
                "type": "long"
            },
            "score": {
                "type": "double"
            },
            "active": {
                "type": "boolean"
            },
            "created_date": {
                "type": "date"
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
{"id":"1","name":"Alice","age":30,"salary":75000,"score":95.5,"active":true,"created_date":"2024-01-15"}
{"index":{"_index":"index-7"}}
{"id":"2","name":"Bob","age":25,"salary":60000,"score":88.3,"active":true,"created_date":"2024-02-20"}
{"index":{"_index":"index-7"}}
{"id":"3","name":"Charlie","age":35,"salary":90000,"score":92.7,"active":false,"created_date":"2024-03-10"}
{"index":{"_index":"index-7"}}
{"id":"4","name":"Diana","age":28,"salary":70000,"score":89.1,"active":true,"created_date":"2024-04-05"}
{"index":{"_index":"index-7"}}
{"id":"5","name":"Bob","age":30,"salary":55000,"score":81.1,"active":true,"created_date":"2024-04-05"}
{"index":{"_index":"index-7"}}
{"id":"5","name":"Diana","age":35,"salary":65000,"score":71.1,"active":true,"created_date":"2024-02-05"}
'
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
  "query": "source=index-7 | stats count(), min(age) as min, max(age) as max, avg(age) as avg"
}'


curl --location --request POST 'http://localhost:9200/_plugins/_ppl' \
--header 'Content-Type: application/json' \
--data-raw '{
  "query": "source=index-7 | stats count() as c by name | sort c"
}'

curl --location --request POST 'http://localhost:9200/_plugins/_ppl' --header 'Content-Type: application/json' --data-raw '{
  "query": "source=index-7 | stats count(), sum(age) as c by name | sort c"
}'

curl --location --request POST 'http://localhost:9200/_plugins/_ppl' --header 'Content-Type: application/json' --data-raw '{
  "query": "source=index-7 | where name = \"Bob\" | stats sum(age)"
}'


curl --location --request POST 'http://localhost:9200/_plugins/_ppl' --header 'Content-Type: application/json' --data-raw '{
  "query": "source=index-7 | stats sum(age) as s by name | sort s"
}'

curl --location --request POST 'http://localhost:9200/_plugins/_ppl' --header 'Content-Type: application/json' --data-raw '{
  "query": "source=index-7 | stats sum(age) as s by name | sort name"
}'

curl --location --request POST 'http://localhost:9200/_plugins/_ppl' \
--header 'Content-Type: application/json' \
--data-raw '{
  "query": "source=index-7 | stats count() as c by name"
}'
```

## Steps to Run Unit Tests for Search Flow

Run the following command in **OpenSearch** to execute tests
```
./gradlew :plugins:engine-datafusion:test --tests "org.opensearch.datafusion.DataFusionReaderManagerTests"
```
