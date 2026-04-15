# Native Arrow Transport Path — Design

## Problem

The current Arrow Flight transport serializes all responses through `VectorStreamOutput`,
which stuffs bytes into a `VarBinaryVector`. This is wasteful when the response already
contains a `VectorSchemaRoot` (e.g., query results from DataFusion). The data gets
serialized to bytes, packed into a binary vector, sent over Flight (which does its own
Arrow IPC), then unpacked and deserialized on the other side.

## Solution: Approach A — Dual-Path in FlightOutboundHandler

Add an `instanceof` check in the outbound handler. If the response implements
`ArrowBatchResponse`, extract the `VectorSchemaRoot` and call `putNext()` directly.
Otherwise, use the existing byte path unchanged.

### New Interfaces

1. **`ArrowBatchResponse`** — marker interface for responses carrying native Arrow data
   - `VectorSchemaRoot getArrowRoot()` — returns the native Arrow data
   - `Schema getArrowSchema()` — returns the Arrow schema

2. **`ArrowStreamHandler<T>`** — marker interface for handlers that can receive native Arrow
   - `T readArrow(VectorSchemaRoot root)` — reads a batch from native Arrow data

### Server-Side Changes

- `FlightOutboundHandler.processBatchTask()` — instanceof check before `writeTo()`
- `FlightServerChannel.sendArrowBatch()` — new method that sends VectorSchemaRoot directly

### Client-Side Changes

- `FlightTransportResponse.nextResponse()` — instanceof check on handler before VectorStreamInput

### Fallback

`ArrowBatchResponse` still implements `writeTo(StreamOutput)` for Netty4 transport.
The fallback path uses `ArrowStreamWriter` to serialize to IPC bytes.

### Scope

Changes are entirely within the `arrow-flight-rpc` plugin. No core OpenSearch changes.
