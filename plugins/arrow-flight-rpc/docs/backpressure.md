# Producer Back-Pressure

`FlightServerChannel.sendResponseBatch` honours gRPC's `isReady()` contract: the
producer thread parks on `BackpressureStrategy.waitForListener` before each batch
is queued, and resumes only once gRPC reports the per-stream outbound buffer has
drained below `setOnReadyThreshold`. Slow consumers throttle producer wall-clock
rather than allowing buffers to accumulate to the point of allocator OOM.

## Why the eventloop queue exists

Multiple producer threads may call `channel.sendResponseBatch(...)` concurrently
on the same stream (concurrent segment search, parallel batch generation). Arrow
Flight's `ServerStreamListener.putNext` is **not** safe for concurrent calls and
the contract requires `start → putNext × N → completed` in strict order on a
single thread; out-of-order or interleaved calls corrupt the stream.

Each `FlightServerChannel` therefore funnels submissions through a per-channel
single-threaded executor (the "eventloop"). Producer threads enqueue
`BatchTask`s; the eventloop dequeues them and performs the actual zero-copy
transfer plus `putNext`. This serialises ordering without making producer threads
contend on a per-channel mutex.

The back-pressure gate runs on the producer thread *before* the eventloop
submission, so a slow consumer throttles allocation rather than letting the
queue grow.

## Settings

| Setting | Default | Property |
|---|---|---|
| `arrow.flight.channel.ready_timeout` | `60s` | node-scope |
| `arrow.flight.channel.outbound_buffer_threshold` | `64mb` | node-scope |

- `ready_timeout` caps how long the producer thread parks before failing the
  batch with `StreamErrorCode.TIMED_OUT`. `100ms` minimum.
- `outbound_buffer_threshold` is the per-stream gRPC buffered-bytes watermark.
  `isReady()` flips false once the per-stream outbound queue crosses this size.
  **Must be strictly smaller than `native.allocator.pool.flight.max`** so the
  gate engages before the allocator runs out (typical headroom: ~16 MiB).

## Operator-facing behaviour

### Fast consumer (steady state)
The readiness check is essentially free: the producer's call to
`awaitReadyOrThrow` returns immediately because gRPC's outbound buffer stays
below threshold.

### Slow consumer
- `isReady()` flips false once gRPC's per-stream outbound buffered-bytes crosses
  the threshold. The producer's next `sendResponseBatch` parks.
- gRPC fires `OnReadyHandler` after the consumer drains some bytes from the wire
  (HTTP/2 `WINDOW_UPDATE` arriving). The producer wakes and resumes.
- Sustained slowness for longer than `ready_timeout` causes the batch to fail
  with `TIMED_OUT`; the producer thread is freed and the stream terminates.

### Cancellation
Client-cancel propagates via gRPC's `OnCancelHandler` into the strategy's cancel
callback. Any thread parked on `waitForListener` wakes promptly with
`StreamErrorCode.CANCELLED`.

## Tuning and operational concerns

### Sizing the flight pool

`native.allocator.pool.flight.max` is **per node**, shared across all active
streams. `outbound_buffer_threshold` is **per stream**. Rule of thumb:

```
flight pool max  >=  N concurrent streams × (threshold + ~16 MiB headroom)
```

For N=10 concurrent streams at the default 64 MiB threshold, that's roughly
800 MiB. The threshold itself is a watermark, not a max-message-size — a single
batch larger than the threshold ships in one shot, with the producer parking on
the next `sendResponseBatch`. The hard constraint on per-batch size is the pool
cap (allocation must fit).

### Thread-pool exhaustion under slow consumers

The producer thread parks while waiting for the consumer. The action handler
runs on whichever thread pool it was registered against (typically `SEARCH` or
`GENERIC`); under N concurrent slow streams, N threads from that pool are
parked simultaneously. Once the pool is exhausted, new requests targeting it
queue up or are rejected.

Mitigations:
- Size `ready_timeout` to fail unresponsive streams within an acceptable window.
- Register stream actions on a pool sized for streaming workloads (not on
  shared CPU-bound pools): a pool of K threads both bounds concurrent streams
  to K and isolates the parking from unrelated traffic. A dedicated
  admission-control layer is a more flexible alternative, out of scope here.

### ⚠️ Known limitation: unbounded eventloop queue

`awaitReadyOrThrow` checks `listener.isReady()`, which reflects only gRPC's
per-stream outbound buffer. The actual push to gRPC happens later, on the
per-channel eventloop, when the eventloop dequeues a `BatchTask` and calls
`putNext`. Between enqueue and `putNext`, the in-flight batch sits in our own
queue — invisible to gRPC, so `isReady()` doesn't account for it.

Implication: a producer that allocates batches significantly faster than the
eventloop drains can pile up retained batches in the queue *before* gRPC's
outbound crosses the threshold and `isReady()` flips false. The flight pool
allocator can be pushed past `outbound_buffer_threshold` by roughly the size
of the queued-but-not-yet-pushed batches. The current `flight pool max` sizing
guidance (threshold + ~16 MiB headroom per stream) absorbs typical bursts but
doesn't formally bound this.

A byte-aware bounded queue at the eventloop entry — that parks (or rejects)
the producer when the sum of queued batch sizes crosses a per-channel cap —
would close the gap. Open design questions:

- **Cap dimension**: byte-aware (sum of retained sizes) vs depth-aware (count).
  Bytes is correct for OOM protection.
- **Mechanism**: park the producer at enqueue (mirrors `awaitReadyOrThrow`'s
  shape; same thread-pool-exhaustion caveat applies) vs reject with
  `RESOURCE_EXHAUSTED`.
- **Sizing**: per-channel cap (independent) vs carve from a shared per-node
  budget tied to `flight pool max`.

Tracked separately; not addressed in this change.

### ⚠️ Virtual threads for park-bound producers

> **For workloads where the producer is mostly bottlenecked on
> `awaitReadyOrThrow` (lots of parking, little compute per batch), the code
> that registers the stream action can dispatch its handler on a virtual-thread
> executor it owns.** Park time then doesn't consume a platform thread.
> CPU-bound work (e.g. building each batch) should still run on a sized
> platform-thread pool to avoid pinning the carrier — typically by submitting
> that work to a separate executor and awaiting the result from the virtual
> thread. The framework itself does not assume virtual threads; this is a
> decision for the action's registrant.
