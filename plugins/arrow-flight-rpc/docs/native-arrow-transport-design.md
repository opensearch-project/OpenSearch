# Native Arrow Transport Path

## Overview

The Arrow Flight transport supports a native Arrow path where typed `VectorSchemaRoot` data
flows directly over Flight without byte serialization. This is for APIs that produce
Arrow-columnar data natively (e.g., query engines like DataFusion).

The existing byte-serialized path (`writeTo`/`read` via `StreamOutput`/`StreamInput`) is unchanged.

## Quick Start

### 1. Define your response

Extend `ArrowBatchResponse`. No `writeTo`/`read` override needed — the framework handles it.

```java
public class MyQueryResponse extends ArrowBatchResponse {
    public MyQueryResponse(VectorSchemaRoot root) { super(root); }
    public MyQueryResponse(StreamInput in) throws IOException { super(in); }
}
```

### 2. Server-side handler — produce Arrow data

```java
void handleRequest(MyRequest request, TransportChannel channel, Task task) throws IOException {
    // Get the channel's allocator. Use this directly for producer roots
    // (not a child allocator) to avoid Arrow's cross-allocator transfer bug
    // with foreign-backed buffers from C data import.
    BufferAllocator allocator = ArrowFlightChannel.from(channel).getAllocator();

    Schema schema = new Schema(List.of(
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
    ));

    try {
        for (int i = 0; i < batchCount; i++) {
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            // populate vectors...
            channel.sendResponseBatch(new MyQueryResponse(root));
            // root is now owned by the framework — don't reuse or close it
        }
        // Cleanup callback runs on executor after all batches are flushed.
        channel.completeStream();
    } catch (Exception e) {
        channel.sendResponse(e);
    }
}
```

### 3. Client-side handler — consume Arrow data

```java
class MyQueryHandler implements StreamTransportResponseHandler<MyQueryResponse> {

    public MyQueryResponse read(StreamInput in) throws IOException {
        return new MyQueryResponse(in);
    }

    public void handleStreamResponse(StreamTransportResponse<MyQueryResponse> stream) {
        MyQueryResponse response;
        while ((response = stream.nextResponse()) != null) {
            VectorSchemaRoot root = response.getRoot();
            VarCharVector names = (VarCharVector) root.getVector("name");
            Float8Vector scores = (Float8Vector) root.getVector("score");
            // process typed vectors...
        }
        stream.close();
    }

    public void handleException(TransportException exp) { /* handle error */ }
    public String executor() { return ThreadPool.Names.GENERIC; }
}
```

## Allocator Management

All producer roots must share the same root allocator as the channel for zero-copy
transfer to work. Use `ArrowFlightChannel.from(channel).getAllocator()` to get the
channel's allocator, then create a child allocator for per-request memory isolation.

### Important: C Data Import and allocator choice

If your producer imports data via Arrow's C Data Interface (`Data.importIntoVector`,
`Data.importIntoVectorSchemaRoot`), the imported buffers are foreign-backed. Arrow Java
has a bug where cross-allocator `transferOwnership` of foreign-backed buffers doesn't
properly release the internal `ArrowArray` C struct buffer (128 bytes per import call),
causing a memory leak in the source allocator.

**To avoid this**, create producer roots from the **channel allocator directly** (not a
child allocator). The framework creates the shared Flight root from the same allocator
as the producer, ensuring same-allocator transfer which avoids the bug.

If you need per-request memory isolation, create a child allocator for tracking but
use the channel allocator for the actual Arrow data buffers.

### Using `completeStream(Closeable)` for cleanup

Register a cleanup callback that runs on the executor after all batches are flushed
and the shared root is closed:

```java
BufferAllocator channelAllocator = ArrowFlightChannel.from(channel).getAllocator();

for (batch : batches) {
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, channelAllocator);
    // populate...
    channel.sendResponseBatch(new MyResponse(root));
}

// Register cleanup — runs on the executor after all batches are flushed.
channel.completeStream(requestAllocator::close);
```

## Ownership Contract

| Resource | Created by | Closed by |
|----------|-----------|-----------|
| Channel allocator | Framework | Framework (on channel close) |
| Child allocator (optional) | Producer | Framework (via `completeStream(Closeable)` callback) |
| Producer root (per batch) | Producer | Framework (after zero-copy transfer on executor) |
| Shared Flight root | Framework | Framework (on channel close) |

After calling `sendResponseBatch(response)`, the framework owns the response's root.
Do not reuse or close it — the framework transfers its buffers and closes it on the executor.

## Pipelining

Batches can be produced in parallel. Each batch must have its own `VectorSchemaRoot`
(created from the channel's allocator). The framework serializes the transfer and send
on the executor thread. The producer can queue batches without waiting for each to flush.
