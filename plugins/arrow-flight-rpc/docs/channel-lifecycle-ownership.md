# FlightServerChannel Lifecycle & Buffer-Ownership Contract

## Purpose

Server-side Arrow Flight streaming frees off-heap Arrow buffers and terminates a gRPC
call from **multiple unsynchronized threads at once**. Without a single, explicit
ownership model this produces several classes of bug:

1. **Double-close crash** — `close()` runs its teardown twice, double-firing the
   TaskManager cancellable-channel untrack listener → `AssertionError` (with `-ea`) that
   kills the node.
2. **Stream-root use-after-free / double-free** — a concurrent cancel frees the reused
   stream root while the producer is mid-`transferRoot` or mid-`putNext` (gRPC still holds
   a zero-copy reference).
3. **Stream-root / source-root leak** — buffers transferred into the reused root after a
   concurrent close, or a source root for a batch whose send never runs, are owned by
   nobody.
4. **Consumer hang** — a batch send fails, the batch is dropped, no schema frame ever
   reaches the client, and the consumer's `FlightStream.getRoot()` blocks forever.

This document is the **authoritative model**. It was validated by an exhaustive
case-sweep (98 enumerated cases across buffer / thread-interleaving / method-exit /
lifecycle-state axes; 64 confirmed defects in the current code, collapsing to **12
distinct root-cause classes** — see §9). The implementation in `FlightServerChannel` and
`FlightOutboundHandler` MUST conform to it, and §9's coverage matrix MUST stay true.

---

## 1. Threads

All concurrency reduces to **three** thread classes per channel. The distinction between the
first two is load-bearing: the gap between them is where source roots can orphan (§6 S7).

| Name | What it is | What it runs |
|------|------------|--------------|
| **`T_caller`** | The producer's calling thread (e.g. a `SEARCH` worker) inside `FlightOutboundHandler.sendResponseBatch`. | `awaitReadyOrThrow()` (the back-pressure gate) and `executor.execute(task)`. **This is where a batch is built but not yet handed to the channel.** |
| **`T_exec`** | The channel's **single-thread** flight executor (`FlightTransport.getNextFlightExecutor()`). | Everything the handler submits: `processBatchTask`→`sendBatch`, `processCompleteTask`→`completeStream`, `processErrorTask`→`sendError`, `failStream`. Also `ArrowFlightProducer.getStream`'s bootstrap. |
| **`T_grpc`** | gRPC's cancel-delivery thread (`cancelExecutor` in `JumpToApplicationThreadServerStreamListener`). | `onChannelCancelled()` → `close()`. |

**Key consequences:**
- The flight executor is single-threaded, so **no two `T_exec` ops run concurrently with
  each other.** Genuine concurrency is only `T_caller`/`T_exec`/bootstrap **vs** `T_grpc`.
- `T_caller` and `T_exec` are **different threads**: a batch can be parked in the gate on
  `T_caller` while a *prior* batch runs on `T_exec`. The window between "batch built on
  `T_caller`" and "`sendBatch` runs on `T_exec`" is where source roots orphan (§6 S7).

### Single-writer model

The stream root is the only state genuinely contended across threads: `T_exec` *uses* it
during transfer/`putNext`, while a cancel wants to *free* it. The channel resolves this by
**confining the root to a single thread** rather than synchronizing access to it:

- **The stream `root` and every `serverStreamListener` call live only on `T_exec`** — created,
  filled, and freed there. Because the executor is single-threaded, batch sends, terminal ops,
  and the root free are serialized by the executor itself; none can interleave.
- **`close()` is a signal, not a buffer operation.** From any thread it flips `open` once
  (CAS), fires close listeners, and **posts the root free onto `T_exec`**:
  `executor.execute(() -> { if (root != null) { root.close(); root = null; } })`. Because the
  executor is FIFO, that free runs *after* any in-flight send completes. `close()` touches no
  Arrow buffers and no gRPC from the caller's (possibly `T_grpc`) thread.

**This is deadlock-free by construction:** nothing is ever held across a gRPC/Arrow/blocking
call. The only mutex (`closeListenerMutex`, §4) is a leaf — its critical section only mutates an
`ArrayList`. `close()` from `T_grpc` never blocks: `execute()` enqueues and returns. There is no
waits-for cycle to construct. The single invariant a future change must preserve is the
self-evident **"`root` and `serverStreamListener` are touched only on the flight executor."**

The one trade-off is that `close()` frees the root **asynchronously** (on the executor) rather
than inline — see §10.

---

## 2. Roles & responsibilities

| Component | Role | Owns | MUST NOT |
|-----------|------|------|----------|
| **`FlightServerChannel`** | **Sole owner** of all Arrow buffers and the gRPC stream lifecycle. The only place a root is freed and the only place a terminal gRPC op is issued. | stream `root`, gRPC listener, `terminalSent`, close listeners | leak/double-free a root; issue two terminal ops; act on the stream after terminal or after `cancelled` |
| **`FlightOutboundHandler`** | **Stateless translator/dispatcher.** Thread-switch onto `T_exec`, back-pressure gate, translate response→channel call, relay outcome. Owns the **hand-off discipline** (§3) for the source root until the channel takes it. | nothing persistent | touch/free the *stream* root; call `close()` directly; serialize a response itself |
| **`FlightTransportChannel`** | Thin framework adapter. `release()` → `channel.close()`. | nothing | contain its own close/idempotency logic |
| **`ArrowFlightProducer`** | Per-call bootstrap. May `close()` on early/inbound errors. | nothing | assume "double close is harmless" without the channel guaranteeing it |
| **`T_grpc` (cancel)** | Signals cancellation → `close()`. | — | — |

**Pivotal rule:** the handler hands the channel a `TransportResponse` and a **header
supplier**, and receives back success or an exception. It never frees the *stream* root,
never serializes the header itself, and never closes the channel directly. Its one buffer
responsibility is the **source-root hand-off** (§3.1).

---

## 3. Buffer ownership — every buffer freed exactly once

| Buffer | Created by | Freed by | When |
|--------|-----------|----------|------|
| **Source root** — `ArrowBatchResponse.getRoot()`, the producer's per-batch native batch | the caller/producer | **the channel** — via `sendBatch`'s `finally` if handed off, else via `channel.releaseUnsent(response)` on the failed-handoff path (§3.1) | exactly once |
| **Stream root** — the single reused VSR (native *or* byte) sent on the wire | the channel, lazily on the first batch | **the channel**, in `close()` only | terminal, exactly once |
| **Metadata buf** — per-batch app metadata on the Flight frame | the channel, per batch | gRPC via `putNext(ArrowBuf)`, or the channel if `putNext` throws before hand-off (§3.3) | handoff |
| **Byte-serialization vector** — the `VarBinary` vector of the byte stream root | the channel (it *is* the stream root's vector) | **the channel**, in `close()` | terminal, exactly once — **never per-batch** (§3.2) |

**Zero-copy retention:** native stream-root buffers are **not** freed right after `putNext`
— gRPC may still hold zero-copy refs. They are freed once, at `close()`, after the
framework's release boundary. The reused stream root lives from the first batch until
`close()`.

### 3.1 Source-root hand-off discipline (closes S7 — the orphan class)

The source root has **two** potential free sites on **two** threads, and the gap between
them is unguarded. Ownership transfers to the channel at **`sendResponseBatch` entry**, and
the handler guarantees the source is freed in **exactly one** of two mutually-exclusive
places via a `handedOff` flag:

```
sendResponseBatch(response, ...):                      // T_caller
  handedOff = false
  try:
    if (!(channel instanceof FlightServerChannel)) { relay error; return; }   // handedOff stays false
    flightChannel.awaitReadyOrThrow()                  // may throw CANCELLED/TIMED_OUT
    flightChannel.getExecutor().execute(task)          // may throw RejectedExecutionException
    handedOff = true                                   // sendBatch now owns it; its finally frees it
  finally:
    if (!handedOff) flightChannel.releaseUnsent(response)   // free here, exactly once
```

- **Handed off** → `sendBatch` runs on `T_exec` → its `finally` frees the source (§3.4).
- **Not handed off** (gate throws, `execute` rejects, or wrong-channel-type early return)
  → handler frees via `releaseUnsent`.
- Mutually exclusive ⇒ **freed exactly once.**

`releaseUnsent(response)` is a channel method (keeps "only the channel frees Arrow
buffers" intact): it closes `response.getRoot()` **iff** `response instanceof
ArrowBatchResponse` (the byte path has no off-heap source). It does not touch the stream
root or lifecycle.

> A second hand-off gap is **inside** `processBatchTask` on `T_exec`: `getHeaderBuffer`
> (which `throws IOException`) and `VectorSchemaRoot.create`/`transferRoot` can throw
> **before** the channel adopts the source. This is handled inside `sendBatch` itself
> (§3.4), not by the handler — see the unified single-entry contract in §5.

### 3.2 Byte path: stream-root vector freed once, never per-batch

The byte-serialized `VectorStreamOutput.ByteSerialized` vector **is** the stream root's
`VarBinary` vector once adopted. Freeing it per batch (today's `try (out)`) frees buffers
gRPC may still be draining (zero-copy) and races `close()`. The channel reuses the vector
across batches (`reset()`/`setValueCount(0)`) and frees it once in `close()`.
`ByteSerialized.close()` must free **only** a vector it itself created (the first-batch,
`existingRoot==null` case); when adopting the channel's existing root it releases nothing.

### 3.3 Metadata buf: freed if `putNext` throws before hand-off

```
buf = allocator.buffer(len); buf.writeBytes(metadata)
try { listener.putNext(buf) } catch (t) { if (buf.refCnt() > 0) buf.close(); throw t }
```
Covers the windows where Flight never adopts the buffer (`waitUntilStreamReady` throw,
`ArrowMessage` ctor throw). The `refCnt()` guard tolerates the narrow `onNext`-throw
subwindow where Flight's `ArrowMessage.close()` already released it.

### 3.4 Source-root free inside `sendBatch` (covers transfer/create/header throws)

```
finally: if (native && sourceRoot != null) sourceRoot.close()
```
The native source root is owned exclusively by this `sendBatch` call, so it is freed
**unconditionally** in the `finally` on every exit: after a successful transfer it is empty
(close is a buffer no-op), and on any throw before the transfer — empty-field-vectors check,
`VectorSchemaRoot.create` OOM, `transferRoot` mid-loop, the header build — it still holds its
buffers (close frees them). **A bare `producerRoot.close()` on the happy path plus a catch-only
cleanup is insufficient** (today's bug): the free must be in a `finally` covering every exit.
On the native **first batch**, the freshly-created stream root is freed by
`transferIntoStreamRoot`'s own try/finally if transfer fails before adoption, so an un-adopted
stream root cannot leak before becoming `root`.

### 3.5 Adopted stream root after a non-`Exception` `Throwable` in `sendBatch`

`serverStreamListener.start`/`putNext` and `transferRoot` can throw a non-`Exception`
`Throwable` — `OutOfMemoryError` under memory pressure, or an `AssertionError` from Arrow/gRPC
under `-ea`. If this happens **after** the stream root is adopted (`root != null`),
`sendBatch`'s `finally` frees only the *source* root; the *stream* root is freed only by
`close()`. The batch task is non-terminal, so `BatchTask.close()` does not release the channel —
nothing would call `close()` and the adopted stream root would leak. `processBatchTask` therefore
catches `Throwable` (after `FlightRuntimeException`/`Exception`) and routes it through `failStream`
(which releases the channel → `close()` posts the stream-root free) and reports it to the message
listener (wrapped to honour the listener's `Exception` contract) rather than letting it escape and
kill the executor worker. `failStream` closes the channel directly when there is no transport
channel, so `close()` runs on every variant.

```java
private final AtomicBoolean open;          // "resources not yet freed"; flips true→false ONCE via CAS
private volatile boolean cancelled;        // set once by T_grpc in onChannelCancelled
private volatile VectorSchemaRoot root;    // reused native|byte stream root; T_exec-confined; nulled after free
private boolean terminalSent;              // completed() XOR error() issued; T_exec-confined
private final Object closeListenerMutex;   // leaf mutex: guards closeListeners + closeListenersFired only
private final List<ActionListener<Void>> closeListeners;
private boolean closeListenersFired;       // guarded by closeListenerMutex
```

- **`open`** — the teardown gate. `compareAndSet(true,false)` admits **exactly one** thread
  into teardown; every other `close()` caller returns immediately. The CAS winner is the sole
  poster of the root free and sole firer of close listeners. **Thread-agnostic.**
- **`cancelled`** — "client gone; gRPC already terminated the call." Read by `sendBatch`,
  `completeStream`, `sendError` to reject/no-op.
- **`root`** — touched **only on `T_exec`** (created, filled, freed there). `volatile` only so the
  test-only `getRoot()` reads a consistent reference; correctness rests on executor confinement,
  not volatility. Nulled immediately after free so nothing re-frees or re-fills it.
- **`terminalSent`** — `T_exec`-confined; at most one terminal gRPC op per stream.
- **`closeListenerMutex`** — a leaf mutex; its critical section only mutates the listener list +
  `closeListenersFired`, never a gRPC/Arrow/blocking call. It is the *only* mutex in the class.

### Invariants (must hold under every interleaving)

1. `notifyCloseListeners()` runs **exactly once** (CAS winner only) → TaskManager untrack
   fires once → no `AssertionError`, any thread, any order.
2. `root.close()` runs **exactly once** — only in the free task the CAS-winning `close()` posts
   to `T_exec`; `root` is nulled there. (If a send adopted no root, the task is a no-op.)
3. **At most one terminal gRPC op** (`completed` XOR `error`) per stream, and **never after
   `cancelled`** (`completeStream`/`sendError` check `cancelled`, both on `T_exec`).
4. **No root freed under an in-flight send** — the free runs on `T_exec` behind any in-flight
   send by the executor's FIFO ordering; root use and root free never overlap (single-writer).
5. **No source root leaked or double-freed** — freed exactly once via §3.1 (handoff) +
   §3.4 (in-send `finally`), mutually exclusive.
6. **No deadlock** — no lock is held across any gRPC/Arrow/blocking call; the only mutex
   (`closeListenerMutex`) is a leaf; `close()` only `execute()`s and returns. No waits-for cycle.
7. **Close-listener registration is safe** — `addCloseListener` either fires immediately
   (listeners already fired) or enqueues under `closeListenerMutex`, with no lost/duplicate fire.
8. **A throwing close listener cannot skip the rest** — `notifyCloseListeners` isolates each
   listener in try/catch so one failure can't strand TaskManager untrack.
9. **Single-writer invariant (the property future changes must preserve):** `root` and every
   `serverStreamListener` call are touched **only on `T_exec`**.

---

## 5. Method contracts

All of `sendBatch`/`completeStream`/`sendError` run on `T_exec` and take **no lock** — the
single-threaded executor serializes them with each other and with the `close()`-posted root free.

### `sendBatch(TransportResponse response, CheckedSupplier<ByteBuffer> header)` — `T_exec`, no lock

Single entry; native vs byte decided **inside** the channel. The header is built via a
supplier invoked **before any root is mutated**, so a header-build `IOException` fails fast —
the source is freed by the `finally` and no stream root is adopted. The handler never
serializes. The native source root is freed once on every exit by the outer `finally`
(unconditional: empty after a successful transfer → close is a no-op; full on an early throw →
close frees it).

```
sourceRoot = (response instanceof ArrowBatchResponse) ? response.getRoot() : null
try:
  if (cancelled)   throw StreamException.cancelled(...)   // reject → finally frees source
  if (!open.get()) throw IllegalStateException(...)       // reject → finally frees source
  firstBatch = (root == null)
  hdr = header.get()                                      // built FIRST; throw → finally, no root mutated
  if (native):
    if (firstBatch) root = create VSR from sourceRoot's allocator   // create+transfer guarded so an
    VectorTransfer.transferRoot(sourceRoot -> root)                 // un-adopted first root can't leak
  else (byte):
    if (firstBatch) root = new VarBinary VSR
    else            reset root's vector
    response.writeTo(serializer over root)
  if (firstBatch): middleware.setHeader(hdr); listener.start(root)
  if (metadata != null): buf=alloc; writeBytes; try{ listener.putNext(buf) } catch(t){ buf.close(); throw t }
  else: listener.putNext()
  batchNumber++; record stats
finally:
  if (native && sourceRoot != null) sourceRoot.close()    // every exit frees source ONCE
```

### `completeStream(CheckedSupplier<ByteBuffer> header)` — terminal, idempotent, `T_exec`, no lock

```
if (!open.get() || cancelled || terminalSent) { record/log; return; }   // benign no-op
if (root == null) middleware.setHeader(header.get())   // empty stream
listener.completed(); terminalSent = true
recordCallEnd(OK)
```
Does **not** free `root` (free deferred to `close()`, §3 retention). The `cancelled` guard
closes the "terminal-after-cancel" race.

### `sendError(CheckedSupplier<ByteBuffer> header, Exception error)` — terminal, idempotent, `T_exec`, no lock

```
if (!open.get() || cancelled || terminalSent) { record/log; return; }   // benign no-op
flightExc = map(error); middleware.setHeader(header.get())
listener.error(flightExc); terminalSent = true
recordCallEnd(mapped)
```

### `releaseUnsent(TransportResponse response)` — `T_caller`/`T_exec` failed-handoff (§3.1)

```
if (response instanceof ArrowBatchResponse r && r.getRoot() != null) r.getRoot().close();
// never touches stream root, terminalSent, or open
```

### `close()` — fire-once, idempotent, ANY thread

```
if (!open.compareAndSet(true, false)) return;          // losers exit at the door
executor.execute(() -> { if (root != null) { root.close(); root = null; } })   // free ON T_exec, behind any send
// (catch RejectedExecutionException on shutdown — see §10)
notifyCloseListeners();                                // runs exactly once; touches no Arrow buffers
```
`close()` posts the root free to `T_exec` rather than freeing inline, so it never touches Arrow
buffers or gRPC from the (possibly `T_grpc`) caller thread, and the free runs behind any
in-flight send by the executor's FIFO ordering.

### `addCloseListener(listener)` — registration atomic vs `notifyCloseListeners`

```
synchronized (closeListenerMutex):
  alreadyFired = closeListenersFired
  if (!alreadyFired) closeListeners.add(listener)
if (alreadyFired) listener.onResponse(null)            // fire outside the mutex
```
Prevents the register-vs-fire race (lost fire / double fire).

### `notifyCloseListeners()` — fault-isolated

```
synchronized (closeListenerMutex): snapshot = copy(closeListeners); clear(); closeListenersFired = true
for (l : snapshot) try { l.onResponse(null) } catch (e) { log; continue }   // one throw can't strand others
```

### `onChannelCancelled()` — `T_grpc`

```
if (cancelled) return;
cancelled = true; recordCallEnd(CANCELLED); close();
```

---

## 6. Scenario walk-through

`✓` = happens exactly once / as intended.

- **S1 Normal multi-batch success** — `sendBatch×N` (each frees its source ✓) →
  `completeStream` (`completed()` ✓) → `release()→close()` (free root ✓, notify ✓).
- **S2 App error / batch-send failure** — send throws → `sendError` (`error()` ✓) →
  `release()→close()`. Consumer gets an error frame, no hang.
- **S3 Client cancel, idle channel** — `T_grpc close()` frees root ✓, notify ✓. Later queued
  `sendBatch` sees `cancelled` → throws → `finally` frees source ✓. Later
  `completeStream`/`sendError` → `cancelled` no-op ✓.
- **S4 Cancel races an in-flight `sendBatch` (UAF/double-free)** — `T_exec` is mid
  transfer+`putNext` (occupying the single executor thread); `T_grpc close()` wins the CAS,
  **posts the root free onto the busy executor**, and returns without touching buffers. The
  free runs only after `sendBatch` finishes (FIFO) → frees root ✓ strictly after `putNext`. No
  UAF, freed once, notify once.
- **S5 Cancel races a `release()`-driven close (the crash)** — both call `close()`;
  `compareAndSet` admits one; the other returns. notify once ✓ → no `AssertionError`.
- **S6 Early/inbound error in `getStream`** — `listener.error` then `channel.close()`; CAS
  admits one closer even if `T_grpc` also cancels ✓.
- **S7 Cancel/timeout while a batch is parked in the gate (the orphan)** —
  `awaitReadyOrThrow()` throws on `T_caller` **before** `execute`; `handedOff==false` →
  handler's `finally` calls `releaseUnsent` → source freed ✓. Same for `execute` rejection
  and the wrong-channel early return.
- **S8 `getHeaderBuffer`/`transferRoot`/`create` throws inside `sendBatch`** — lands in the
  `finally`; source freed once ✓; no stream root adopted (or freed by a later `close()` if
  adopted). No orphan.
- **S9 Byte multi-batch + cancel** — vector reused across batches on `T_exec`; freed once at
  `close()` (on `T_exec`, behind any send) ✓; no per-batch free, no write-vs-free race.
- **S10 Non-`Exception` `Throwable` after stream-root adoption** — `putNext`/`start`/transfer
  throws OOM/`AssertionError` once `root != null`. `sendBatch`'s `finally` frees the source ✓;
  `processBatchTask`'s `catch (Throwable)` runs `failStream` → channel released → `close()` posts
  the stream-root free ✓; listener notified, worker survives. No leak (§3.5).

---

## 7. Design decisions (explicit)

1. **Terminal op separate from root-free.** `completeStream`/`sendError` issue the gRPC
   terminal op and set `terminalSent`; `root` is freed later in `close()`, preserving the
   zero-copy drain window. (Folding free into `completeStream` shrinks that window — not
   chosen.)
2. **Single `sendBatch(response, headerSupplier)` entry.** Native/byte decided in the
   channel; the handler never touches a stream root and never serializes the header.
3. **Header built by the channel via a supplier, before any root is mutated.** This is
   correctness-critical (not cosmetic): `getHeaderBuffer` throws `IOException`, and building it
   inside `sendBatch` puts that throw inside the source-freeing `finally` — and building it
   before the transfer means a header failure adopts no stream root.
4. **Single-writer confinement, not synchronization.** The stream root and all
   `serverStreamListener` calls are confined to the flight executor; `close()` posts the root
   free there rather than coordinating concurrent access to it. This makes the design
   deadlock-free by construction (nothing is held across a gRPC/Arrow call) at the cost of an
   asynchronous root free (§10). The invariant future changes must keep is the self-evident
   "root is touched only on the executor."

---

## 8. Relationship to the consumer-hang fix (PR #22117)

PR #22117 fixes **S2's symptom** (a failed batch send calls `sendError`+release so the
consumer gets an error, not a hang) and relocates the native transfer into the channel.
That behaviour is preserved here. What this contract adds is the **concurrency safety** PR
#22117 lacks: it leaves `close()` non-atomic and the stream root accessible from both the
producer and the gRPC cancel thread, so S4 (UAF/double-free) and S5 (crash) remain reachable —
and its new `failStream → releaseChannel → close()` adds *another* concurrent `close()` caller.
This model closes S4/S5/S7–S9 while keeping S2 fixed.

---

## 9. Coverage matrix — the 12 distinct root-cause classes

The case-sweep confirmed 64 defects in the current code; verified-and-deduplicated they are
12 classes. Each MUST map to a model mechanism. (`#` = confirmed-defect count folded in.)

| # | Root-cause class | Sev | Bug type | Closed by |
|---|------------------|-----|----------|-----------|
| 1 | Non-atomic `close()` double-entry → double-free root + double `notifyCloseListeners` (crash) | blocker | crash/double-free | `open.compareAndSet` (inv 1,2); S5 |
| 2 | Cancel frees stream root mid-`transferRoot`/`putNext` (UAF) | blocker | UAF | single-writer: root free posted to `T_exec`, behind any send (inv 4); S4 |
| 3 | Cancel after transfer-into-reused-root, send rejects → transferred buffers leak; `root` never nulled | blocker | leak/UAF | gate **before** transfer + null-after-free; free serialized on `T_exec` (inv 2,4); S4 |
| 4 | Source root orphaned when send never runs: gate throw / `execute` reject / wrong-channel return | blocker | leak | `handedOff` + `releaseUnsent` (§3.1, inv 5); S7 |
| 5 | Source root orphaned inside `sendBatch`: `getHeaderBuffer`/`create`/`transferRoot` throw | major | leak | header built before transfer + `finally` frees source (§3.4, §5); S8 |
| 6 | Byte path: per-batch `out.close()` frees reused vector (UAF vs zero-copy & vs cancel) | blocker | UAF | channel owns vector, freed once in `close()` (§3.2); S9 |
| 7 | Terminal-after-cancel: `completeStream`/`sendError` issue terminal op after cancel | major | illegal transition | `cancelled` guard (inv 3) |
| 8 | Metadata buf leaked when `putNext` throws before hand-off | minor | leak | try/catch + `refCnt` close (§3.3) |
| 9 | `addCloseListener` races `close()` flip+clear → lost fire / CME / tracker-map leak | major | leak/visibility | atomic registration (inv 7, §5) |
| 10 | Throwing close listener strands remaining listeners (TaskManager untrack skipped) | minor | leak | per-listener try/catch (inv 8, §5) |
| 11 | `shutdownNow` drops queued tasks undrained → source roots leak | minor | leak (shutdown-only) | **documented limitation** — node teardown; OS reclaims on exit (§10) |
| 12 | Wrong-channel-type / NOOP-listener early returns (defensive `instanceof`) | info | n/a | covered by §3.1 handoff (handedOff stays false → `releaseUnsent`) |
| 13 | Non-`Exception` `Throwable` (OOM / `AssertionError`) from `start`/`putNext`/transfer **after** stream-root adoption → `processBatchTask` only caught `Exception`, so the channel was never released → adopted stream root leaks | major | leak | `catch (Throwable)` in `processBatchTask` routes through `failStream` (releases channel → `close()` frees the root) + notifies the listener; `failStream` closes the channel directly when there is no transport channel (§3.5); S10 |

---

## 10. Known limitations (not closed by this model)

- **Asynchronous stream-root free (a consequence of the single-writer choice).** Because
  `close()` posts the root free onto the flight executor, the buffers are freed slightly after
  `close()` returns, not inline. Functionally this is the point (it serializes the free behind
  any in-flight send), but two implications follow: (a) tests asserting allocator state after
  `close()` must drain the executor first; (b) if the executor is already shut down when
  `close()` runs, the `execute()` is rejected and the stream root is not freed — see the next
  bullet. This is the deliberate trade the single-writer model makes for being deadlock-free by
  construction (§1, §7 decision 4).
- **Node-shutdown drain gap.** On `executor.shutdownNow()`, queued tasks — including a
  `close()`-posted root free, and any never-run `BatchTask`s — are dropped without running, so
  their roots are not freed. Likewise, if `close()`'s `executor.execute(...)` is rejected
  (executor already shut down), the root free never runs; `close()` logs this at **WARN** (not
  debug) so the shutdown-time leak is visible. We deliberately do **not** free the root inline on
  rejection: a rejection from `shutdown()` refuses new tasks while an already-running send may
  still be touching the root on the executor thread, so freeing from the (possibly gRPC) caller
  thread would reintroduce the use-after-free this model prevents. This is **shutdown-only** (the
  node is stopping; the OS reclaims off-heap memory on process exit), so impact is limited to
  noisy Arrow leak assertions / test flakiness, not a sustained production leak. Closing it fully
  would require draining queued tasks before `shutdownNow`; tracked separately.
- **Final-frame zero-copy drain.** Whether gRPC can still hold a reference to the *last*
  frame's buffers when `close()` frees `root` is a pre-existing concern with the same shape
  in today's code; out of scope here.
```
