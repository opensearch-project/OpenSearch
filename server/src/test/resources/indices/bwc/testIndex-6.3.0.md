# README for _testIndex-6.3.0.zip_

This zip file holds a Lucene index created using ElasticSearch 6.3.0.
It was created by running the underlying commands against a single-node cluster,
then compressing the contents of the underlying Lucene index directory i.e.
the files under `<elasticsearch-root>/data/nodes/0/indices/<index-uuid>/0/index`.
The index contains one document.

## Commands

```
curl -X PUT -H 'Content-Type: application/json' 'localhost:9200/testindex?pretty' -d'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "testData": {
      "properties": {
        "id": { "type": "keyword" },
        "isTestData": { "type": "boolean" },
        "testNum": { "type": "short" },
        "testRange": {"type": "integer_range" },
        "testMessage": {
          "type": "text",
          "fields": {
            "length": {
              "type": "token_count",
              "analyzer": "standard"
            }
          }
        },
        "testBlob": { "type": "binary", "index": false },
        "testDate": { "type": "date" },
        "testLocation": { "type": "geo_point"}
      }
    }
  }
}'

curl -X POST "localhost:9200/testindex/testData/?pretty" -H 'Content-Type: application/json' -d'
{
  "id": "testData1",
  "isTestData": true,
  "testNum": 99,
  "testRange": {
    "gte": 0,
    "lte": 100
  },
  "testMessage": "The OpenSearch Project",
  "testBlob": "VGhlIE9wZW5TZWFyY2ggUHJvamVjdA==",
  "testDate": "1970-01-02",
  "testLocation": "48.553532,-113.022881"
}
'
```
