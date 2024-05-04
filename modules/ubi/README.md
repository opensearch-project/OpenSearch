# User Behavior Insights (UBI)

UBI facilitates storing queries and events for the purposes of improving search relevance.

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

With these optional values, a sample query would look like:

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

If a search request does not contain a `ubi` block in `ext`, the query will *not* be indexed.

Queries are indexed into an index called `ubi_queries`.

## Indexing Events

UBI facilitates indexing both queries and client-side events. These client-side events may be product clicks, scroll-depth,
adding a product to a cart, or other actions. UBI indexes these events in an index called `ubi_events`. This index is
automatically created the first time a query containing a `ubi` section in `ext` (example above).

Client-side events can be indexed into the `ubi_events` index by your method of choice.
