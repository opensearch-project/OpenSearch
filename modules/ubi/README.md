# User Behavior Insights (UBI)

UBI facilitates storing queries and events for the purposes of improving search relevance as described by [[RFC] User Behavior Insights](https://github.com/opensearch-project/OpenSearch/issues/12084).

## Indexes

UBI creates two indexes the first time a search request containing a `ubi` block in the `ext`. The indexes are:
* `ubi_queries` - For storing queries.
* `ubi_events` - For storing client-side events.

## Indexing Queries

For UBI to index a query, add a `ubi` block to the `ext` in the search request containing a `query_id`:

```
curl -s http://localhost:9200/ecommerce/_search -H "Content-type: application/json" -d'
{
  "query": {
    "match": {
      "title": "toner OR ink"
    }
  },
  "ext": {
    "ubi": {
      "query_id": "1234512345"
    }
  }
}
```

There are optional values that can be included in the `ubi` block along with the `query_id`. Those values are:
* `client_id` - A unique identifier for the source of the query. This may represent a user or some other mechanism.
* `user_query` - The user-entered query for this search. For example, in the search request above, the `user_query` may have been `toner ink`.
* `object_id` - The name of a field in the index. The value of this field will be used as the unique identifier for a search hit. If not provided, the value of the search hit `_id` field will be used.

With these optional values, a sample query is:

```
curl -s http://localhost:9200/ecommerce/_search -H "Content-type: application/json" -d'
{
  "query": {
    "match": {
      "title": "toner OR ink"
    }
  },
  "ext": {
    "ubi": {
      "query_id": "1234512345",
      "client_id": "abcdefg",
      "user_query": "toner ink"
    }
  }
}
```

If a search request does not contain a `ubi` block containing a `query_id` in `ext`, the query will *not* be indexed.

## Indexing Events

UBI facilitates indexing both queries and client-side events. These client-side events may be product clicks, scroll-depth,
adding a product to a cart, or other actions. UBI indexes these events in an index called `ubi_events`. This index is
automatically created the first time a query containing a `ubi` section in `ext` (example above).

Client-side events can be indexed into the `ubi_events` index by your method of choice.

## Example Usage of UBI

Do a query over an index:

```
curl http://localhost:9200/ecommerce/_search -H "Content-Type: application/json" -d
{
  "query": {
    "match": {
      "title": "toner OR ink"
    }
  },
  "ext": {
    "ubi": {
      "query_id": "1234512345",
      "client_id": "abcdefg",
      "user_query": "toner ink"
    }
  }
}
```

Look to see the new `ubi_queries` and `ubi_queries` indexes. Note that the `ubi_queries` contains a document and it is the query that was just performed.

```
curl http://localhost:9200/_cat/indices
green open ubi_queries KamFVJmQQBe7ztocj6kIUA 1 0  1 0   4.9kb   4.9kb
green open ecommerce   KFaxwpbiQGWaG7Z8t0G7uA 1 0 25 0 138.4kb 138.4kb
green open ubi_events  af0XlfmxSS-Evi4Xg1XrVg 1 0  0 0    208b    208b
```
