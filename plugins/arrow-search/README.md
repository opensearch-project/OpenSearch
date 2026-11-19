# arrow-search

Streams search results as [Apache Arrow](https://arrow.apache.org/) record batches read from doc values, through the
existing gRPC `SearchService/ServerStreamSearch` method. Clients filter in OpenSearch and receive columnar data that they
can hand to Arrow-native engines (DataFusion, DuckDB, Polars, pandas, Spark) without decoding hits.

Phase one serves filtering use cases: matches are unscored and unordered.

## Design tenets

1. **Minimal client-facing change.** Clients use the existing `ServerStreamSearch` RPC and `SearchRequest` proto, and
   select Arrow with one request header. There is no new port, endpoint, TLS or authentication surface.
2. **Zero copy where the code owns the path.** Doc values are read straight into Arrow vectors. From the coordinator's
   relay to the client's socket, a batch moves by ownership transfer and gathering writes; only small headers are copied.
   The node-to-node hop uses Arrow Flight, which currently copies each batch twice. On send, Flight's gRPC message mixes a
   heap header with the body, so Netty copies the whole message into direct memory. On receive, Flight copies the batch
   into a new buffer.
3. **No marshalling of rows.** Rows travel as native Arrow buffers end to end: Arrow Flight between nodes, Arrow IPC
   messages to the client. Only control objects are serialized.
4. **Batched, never the whole result.** Results are produced and held one batch at a time, so nothing held grows with the
   result size.
5. **Off-heap.** Batches live in Arrow memory pools, not on the heap. The pools have limits, and work that does not fit is
   rejected.
6. **Waits on virtual threads.** Every wait for a consumer parks a virtual thread, never a platform thread.
7. **Reuse the search infrastructure.** The coordinator runs inside `TransportSearchAction` (resolution, routing,
   can-match, throttling, failover, stats, slow logs, workload group admission). Each shard runs a regular query phase
   (rewrite, security hooks, timeouts, cancellation, limits). Node-to-node streaming uses `arrow-flight-rpc`, as streaming
   aggregations do.
8. **Pause/resume, backpressure-driven batch production.** As in a streaming aggregation, each shard runs one query phase
   whose collector sends every batch as soon as it is full. A send waits while the consumer is not ready. This pauses the
   scan with its scorer and doc-values iterators open; when the send returns, the scan resumes at the next match. The query
   is evaluated once per shard, however many batches it produces, and demand flows back from the client to every data
   node.
9. **Parallel per-segment production.** As in a streaming aggregation under concurrent segment search, a shard's query
   phase splits its segments into slices that it collects in parallel. Each slice fills and sends its own batches, with the
   same pause/resume backpressure. Parallelism within a shard follows the concurrent segment search settings (mode and
   slice count). The slices of a shard share its row limit and memory limits. Each slice has its own compute budget, and all
   of them stop once any one slice spends its budget.

## Requirements

- `transport-grpc` enabled (`aux.transport.types: [transport-grpc]`)
- the `arrow-base` and `arrow-flight-rpc` plugins, and the stream transport feature flag
  `opensearch.experimental.feature.transport.stream.enabled`
- this plugin, which extends `arrow-base` and `transport-grpc`

Calls use the gRPC port, interceptors and security of `transport-grpc`; use `secure-transport-grpc` in production. With the
security plugin, the gRPC interceptor authenticates each call (basic auth or JWT in the request metadata), and the call is
authorized like any search. Callers need the `indices:data/read/search*` and `indices:data/read/field_caps` privileges.
Document-level security, field-level security and field masking apply on every shard. The node-to-node Flight listener
must be reachable only by cluster members. With the security plugin, run Flight over TLS, so that each shard request
carries the caller's identity and the peer certificate is verified:

```yaml
aux.transport.types: [secure-transport-grpc]
plugins.security.ssl.aux.secure-transport-grpc.enabled: true      # plus pemkey/pemcert/pemtrustedcas_filepath
flight.ssl.enable: true
transport.stream.type.default: FLIGHT-SECURE
```


## Request

A regular protobuf `SearchRequest` sent to `ServerStreamSearch` with the request header `x-opensearch-format: arrow`.

| Option | Meaning |
|---|---|
| `docvalue_fields` | Required. Field names or wildcard patterns, in output column order. A field with format `list` becomes a list column; other formats are rejected. |
| `query`, `post_filter` | Any query; scoring is disabled. Alias filters and document- and field-level security apply. |
| `size` | Caps the total number of rows. Unset means all matches. |
| `terminate_after` | Caps the rows per shard. |
| `timeout` | Bounds the time each slice of a shard spends computing, not the time it waits for the client. |
| `routing`, `preference`, `allow_partial_search_results`, `max_concurrent_shard_requests`, `cancel_after_time_interval`, indices options | As in `_search`. |

Options that need scores, ordering, paging, fetching or aggregations (`sort`, `from`, `search_after`, `aggs`, `highlight`,
`fields`, `stored_fields`, `_source` other than `false`, `inner_hits`, `scroll`, `pit`, `slice`, `ext`, ...) are rejected
with `INVALID_ARGUMENT`, not ignored. So are search pipelines, including an index's default pipeline, and remote cluster
indices.

## Columns

Fields resolve through field capabilities across the target indices, so every shard produces the same schema. Every shard
reads every column, as nulls where its index does not map the field. So a search may select at most as many fields as the
largest `index.max_docvalue_fields_search` of its target indices; a search that selects more fails with
`INVALID_ARGUMENT`.

| Mapping types | Arrow type |
|---|---|
| `byte`, `short`, `integer`, `long` | `Int8`/`Int16`/`Int32`/`Int64`; mixed widths widen to `Int64` |
| `unsigned_long` | `UInt64` |
| `half_float`, `float` | `Float32`; mixed with `double` or `scaled_float`, `Float64` |
| `double`, `scaled_float` | `Float64` |
| `boolean` | `Bool` |
| `date` | `Timestamp(ms, UTC)`; mixed with `date_nanos`, `Timestamp(ns, UTC)` |
| `date_nanos` | `Timestamp(ns, UTC)` |
| `keyword` | `Utf8` |
| `ip` | `FixedSizeBinary(16)`, IPv4 as IPv4-mapped IPv6 |

The field metadata `opensearch.type` of each column lists the mapping types behind it. A document without a value reads as
null.

A document with several values in a scalar column fails the search. A list column (`format: "list"`) has the type
`List<item: T>` and holds every value in doc-values order, as `docvalue_fields` returns them in `_search`: numbers are
sorted, and keyword and ip values are sorted and deduplicated. Nested and object fields are not supported. Neither are the
fields inside a field that some target index maps as `nested`, even with `include_in_root` or `include_in_parent`: their
values belong to hidden nested documents, not to the matching documents that a scan reads.

A field named explicitly must be mapped in some target index, have doc values and compatible types everywhere it is
mapped, and not be inside a nested field. A pattern skips fields that do not qualify. Columns resolve with the caller's
permissions: a field hidden by field-level security counts as not mapped, as in field capabilities, and a pattern skips
it. A masked field returns its masked values. The exception is an `ip` field: its masked values are not addresses, so it
fails the search with `INVALID_ARGUMENT`.

## Response

Every gRPC message is a valid `SearchResponse`:

1. one message per Arrow IPC message, carrying it in field `100` (`bytes`): first the schema, then record batches;
   concatenated, they form an Arrow IPC stream;
2. a final regular `SearchResponse` with `took`, `_shards`, `timed_out`, `terminated_early` and `hits.total` set to the
   number of rows (relation `gte` when `size`, `terminate_after` or `timeout` cut the results).

Rows from different shards interleave; no order is promised. If the search fails after rows were sent, the call ends with
an error status and the client must discard what it received. The same applies when the call is reset (`CANCELLED`)
because the client stopped reading, which can happen after the search ended (see Backpressure).

## Execution

- **Coordinator.** It resolves the columns, sends the schema, then runs the shard phase through `TransportSearchAction`.
  It opens one Flight stream per shard copy, a bounded number at a time, and prefers the node with the fewest open
  streams. It relays each batch to the client by ownership transfer.
- **Failover.** A shard copy that fails before any of its rows reached the client is retried on the next copy, as in
  `_search`. A shard that relayed rows is never retried, so its failure fails the search. Deterministic errors (a bad
  query, a document with several values in a scalar column, a type conflict) fail the search at once.
- **Data node.** Each shard stream is one query phase on a virtual thread. Under concurrent segment search mode `auto`
  (the default) or `all`, with or without aggregations, the query phase splits the shard's segments into up to
  `search.concurrent.max_slice_count` slices and collects them in parallel. Each slice runs on its own virtual thread,
  not on the `index_searcher` pool. With mode `none`, one slice collects every segment. Each slice's collector reads doc
  values in chunks straight into Arrow vectors and cuts batches to their byte budget. The first batch is small and later
  ones grow, so rows arrive early. The collector sends each batch through Flight. When a send must wait, the slice pauses
  and holds no compute slot. The slices of a shard share its row limit (`size`, `terminate_after`). They all stop when
  the row limit is reached, when any slice spends its `timeout` budget, or when a slice fails. As in `_search`, a shard
  whose matches exactly fill the row limit is not `terminated_early`. To find out, the slice that reaches the limit keeps
  scoring its segment for a match past the limit, only while it holds its compute slot and within its budget. If it stops
  first, or segments that may match are left, the shard reports `terminated_early`, not `timed_out`, since it streamed
  every row the limit allows.
- **Backpressure.** When a client stops reading, every relay stops, Flight stops requesting batches, and each shard scan
  pauses inside its send. If the client stays idle longer than `search.arrow.client_idle_timeout`, the search fails with
  `DEADLINE_EXCEEDED`. Everything the search holds on every node is released at once, except the batches gRPC already
  queued for the call; the client receives those first if it reads again. Those batches, like the last batches of a
  search that succeeded, stay charged to the coordinator's memory until the client reads them. Each is charged as a batch
  of the larger of `batch_size` and `max_row_size`, rounded up as Arrow allocates it (see Settings). The rest of the
  search's reservation is released when the search ends. If the client does not read them within another
  `client_idle_timeout`, its call is reset (`CANCELLED`), which releases them. Only a batch that the transport already
  began writing to the connection stays, still charged, until the client reads it or the connection closes. A reset
  needs the call object that gRPC created. If another plugin's gRPC interceptor wraps calls, the node logs a warning once,
  and unread batches stay, still charged, until the client reads them or its connection closes.

## Settings

| Setting | Default | |
|---|---|---|
| `search.arrow.batch_size` | `1mb` | Size that batches grow to. At most `3mb`, below gRPC's default 4MB message limit. |
| `search.arrow.first_batch_size` | `64kb` | Size of the first batch of each slice of a shard. The size doubles with each batch. |
| `search.arrow.batch_rows` | `65536` | Maximum rows per batch. |
| `search.arrow.max_row_size` | `1mb` | Larger rows fail the search; rows up to `batch_size` are always accepted, so the limit is the larger of the two. A row larger than `batch_size` is sent as a batch of its own, so the coordinator sizes each shard stream for batches of this size. Raising it lowers how many shard streams a search opens at once. Keep it below gRPC's default 4MB client message limit. |
| `search.arrow.max_buffered_bytes_per_request` | `64mb` | Batch memory that one search may buffer on its coordinator. It bounds the search's open shard streams: each stream counts as six batches (five that the transport prefetches and one waiting to be sent), plus one batch per search that gRPC holds while the client reads it. A batch counts as the larger of `batch_size` and `max_row_size`, rounded up as Arrow allocates it: to the next power of two, for sizes below 16mb. That is 1mb by default, which allows ten streams, and 4mb with `batch_size: 3mb`. A search reserves only what the streams it can open at once need: one stream per shard that its routing and preference target, and at most `max_concurrent_shard_requests` per node that holds a copy of them. |
| `search.arrow.coordinator.max_buffered_bytes` | `512mb` | Batch memory of all searches on a coordinator, including batches their clients have not read yet. Searches beyond it are rejected. |
| `search.arrow.client_idle_timeout` | `60s` | How long a client may stop reading, both while the search runs and again after it ends. At least `1s`. |
| `search.arrow.cancel_after_time_interval` | `-1` | Cancels searches that run longer, unless the request sets its own interval. |
| `search.arrow.shard.max_open_scans` | 4 × processors | Maximum shard scans open on a data node. More scans wait, then are rejected and retried elsewhere. |
| `search.arrow.shard.admission_wait` | `5s` | How long a scan waits for an open-scan slot. |
| `search.arrow.shard.max_concurrent_batches` | half the search pool | Compute slots: how many scan slices compute at once on a data node. |
| `search.arrow.shard.max_buffered_bytes` | `512mb` | Batch memory of all scans on a data node (static). A scan whose batch does not fit is rejected and retried elsewhere. |

How many slices a scan collects in parallel follows the concurrent segment search settings of the index:
`search.concurrent_segment_search.mode` and `search.concurrent.max_slice_count` (default: half the processors, at most 4).

## Limitations

- No scoring, sorting or aggregations. System and search-throttled indices are rejected.
- A shard's last batch waits while the slice that filled the row limit keeps scoring for a match past it, as `_search`
  does for `terminate_after`. With a selective, costly query, this delays the end of the stream, up to the `timeout`.
- A segment is never split across slices. Intra-segment partitioning (`partition_strategy: balanced`) applies only to
  queries that opt in, and scan queries such as `match_all`, `term` and `range` do not. So a shard dominated by one large
  segment is collected at about the speed of one thread. Letting scans opt in is a follow-up.
- Each slice holds up to one batch being filled and one being sent, so a scan's memory on the data node grows with its
  slice count; `search.arrow.shard.max_buffered_bytes` bounds the total. Rows much larger than the rows before them still
  cost at most about one batch per keyword or list column: each such column stops reading at the row whose values, added
  to those of the columns before it, overdraw the batch's budget.
- A computing slice also keeps heap buffers for the keyword and ip values it looks up: up to a few times `batch_size` for
  lists of many short values. No memory limit or circuit breaker counts these buffers. The slice frees them whenever it
  stops computing, for example to send a batch, so `search.arrow.shard.max_concurrent_batches` bounds the number of slices
  that hold them.
- The coordinator can cancel a shard stream directly only after the stream's first batch arrives. Before that, a search
  that stops (for example, because it reached its row limit) cancels its tasks on the data nodes. A stream's request may
  reach its node after that cancel, so the search repeats the cancel every second on the nodes of the streams still
  opening. Such a scan may run up to about a second past the stop.
- A paused scan keeps its shard's reader open, like a scroll. A closing shard aborts the scan when the scan resumes or
  when its wait for the coordinator times out.
- A coordinator that vanishes without closing its connections (a host failure, a network partition) cannot release its
  scans. A paused scan keeps its open-scan slot, its reader and its batches until its wait for the coordinator times out,
  after `search.arrow.client_idle_timeout` × (open streams + 1): 11 minutes for a search with ten open streams and the
  default 60s idle timeout. The Flight keepalive (`flight.keepalive.time` plus `flight.keepalive.timeout`) ends the wait
  earlier if it is shorter.
- Arrow search tasks are not tracked for resource use. Their lifetime follows the client, and their CPU runs on virtual
  threads, which per-thread accounting does not measure. Search backpressure does not cancel them. Workload management
  does not count their CPU and heap toward their workload group's usage, on which its rejections are based, and does not
  cancel them. Query insights reports no CPU or memory for them. The node bounds them with the scan and compute limits
  above, which all workload groups share.
- A shard's query-phase stats (`query_time_in_millis`, `query_current`) and its query slow log measure the whole scan,
  including the time it waits for the client. The `timeout` budget does not count that time.
- Payload field `100` is not yet declared in `opensearch-protobufs`.
