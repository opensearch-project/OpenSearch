/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.spi.CloseableIterator;
import org.opensearch.analytics.spi.ShuffleBufferAccess;
import org.opensearch.analytics.spi.ShuffleBufferExceededException;
import org.opensearch.analytics.spi.ShuffleBufferRegistry;
import org.opensearch.analytics.spi.ShuffleSlots;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-node registry of shuffle buffers for hash-shuffle joins. One {@link ShuffleBuffer} per
 * (queryId, targetStageId, partitionIndex).
 *
 * <p>Ported from OLAP's {@code ShuffleManager}
 * ({@code opensearch-olap/.../transport/ShuffleManager.java}). OLAP keys by (queryId, stageId)
 * because its dispatch assigns exactly one shuffle partition per node. analytics-engine supports
 * configurations where a single node hosts multiple partitions of the same stage (small clusters,
 * over-provisioned {@code plugins.analytics.shuffle_partitions}), so partitionIndex must be part
 * of the key — otherwise two partitions' left/right payloads and {@code isLast} markers would
 * land in the same buffer and the join would read cross-partition rows + see false completion.
 *
 * <p>Each {@link ShuffleBuffer} tracks per-side sender completion via {@code CountDownLatch}es and
 * stores accepted chunks. On-heap budget enforcement is owned by the MANAGER, not the buffer:
 * {@link #tryAdmit} gates each chunk against a node-level + per-query byte budget (see
 * {@link #setBudgets}). The consumer calls {@link ShuffleBuffer#awaitReady} before draining.
 *
 * @opensearch.internal
 */
public class ShuffleBufferManager implements ShuffleBufferRegistry {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleBufferManager.class);

    private final Map<String, ShuffleBuffer> buffers = new ConcurrentHashMap<>();

    /**
     * Node-level on-heap shuffle budget and per-query footprints — the OOM-safety bound.
     *
     * <p>The shuffle consumer is buffer-all-then-drain, so a node's live shuffle byte[] is the SUM
     * of every buffer it holds across ALL queries/stages/partitions. A per-buffer cap can't bound
     * that sum (N partitions each under the cap still OOM in aggregate), so admission is gated on:
     * <ul>
     *   <li>{@link #nodeBudgetBytes} — hard ceiling on {@link #totalBytes} (all queries combined).
     *       When a chunk would push the node total past this AND the query itself still fits its
     *       share, the producer is told to back off and RETRY (a soft, transient reject: room frees
     *       when other queries finish and release their buffers).</li>
     *   <li>{@link #perQueryMaxBytes} — hard ceiling on a SINGLE query's footprint
     *       ({@link #perQueryBytes}). A query whose own shuffle exceeds this can never fit even on an
     *       idle node, so it fails FAST and NON-retryably ({@link ShuffleBufferExceededException}).
     *       This is the q17 case (one side wants ~7.4GB).</li>
     * </ul>
     * Default disabled ({@code Long.MAX_VALUE}) until wired from settings at plugin startup.
     *
     * <p><b>Known limitation — concurrent-contention starvation.</b> The plugin wires
     * {@code nodeBudget == perQueryMax} (both = {@code node_budget_percent}% of heap). Two legitimate
     * shuffle queries can then each fit their per-query share yet jointly exceed the node budget: the
     * second keeps getting {@link AdmitResult#REJECT_RETRY} and, if its peer doesn't drain within the
     * sender's bounded retry window ({@link ShuffleSenderRetry}), it FAILS rather than waiting
     * indefinitely. This is an accepted trade-off: the engine's validated workload runs heavy shuffle
     * queries one-at-a-time, and failing a contended query is strictly safer than risking a node OOM.
     * Because node and per-query budgets are currently the SAME value, lowering
     * {@code node_budget_percent} does NOT help two large shuffles coexist (it lowers per-query
     * capacity in lockstep, just failing oversized queries earlier) — the remediation is to serialize
     * large shuffles. Splitting node vs per-query into independent settings (so a node can admit
     * several sub-node-budget queries at once) is the future fix if concurrent shuffle becomes a
     * first-class workload.
     */
    private volatile long nodeBudgetBytes = Long.MAX_VALUE;
    private volatile long perQueryMaxBytes = Long.MAX_VALUE;

    /**
     * Per-slot IN-FLIGHT WINDOW: the maximum bytes that may sit queued-but-undrained on a single
     * (buffer, slot) before admission starts returning {@link AdmitResult#REJECT_RETRY}.
     *
     * <p>This is the bound that makes pipelined shuffle's promise real. The node/per-query budgets
     * above cap the ABSOLUTE footprint (they must, as an OOM backstop), but they are sized as a
     * percentage of heap — 80% by default, i.e. ~24GB on a 30GB heap — so on their own they let one
     * partition accumulate until the node is pinned. The window is a much smaller, drain-rate-coupled
     * cap: producers pace to the consumer, and peak residency becomes a function of the window rather
     * than of partition size.
     *
     * <p>Enforced via REJECT_RETRY rather than by blocking on a bounded queue on purpose: admission
     * runs on a transport thread, and parking transport threads to apply backpressure risks starving
     * the node's thread pool. {@link ShuffleSenderRetry} already implements bounded producer-side
     * retry, so reusing it keeps backpressure off the transport threads.
     */
    private volatile long streamWindowBytes = Long.MAX_VALUE;
    private final AtomicLong totalBytes = new AtomicLong();
    private final Map<String, AtomicLong> perQueryBytes = new ConcurrentHashMap<>();

    /**
     * Disk-spill configuration (default OFF). When {@link #spillEnabled} is true and a chunk would
     * breach the PER-QUERY on-heap budget, {@link #tryAdmit} evicts oldest in-memory chunks of the
     * target side to a per-(query,stage,partition,side) spill file under {@link #spillDir} instead of
     * throwing — so the on-heap footprint stays bounded by the budget while the rest lives on disk.
     * The consumer drains spilled chunks back (in arrival order) then the in-memory tail, preserving
     * the buffer-all contract. {@link #spillMaxBytes} is the hard disk ceiling across all of this
     * node's spill files; hitting it (or a disk write I/O error) is the new terminal failure
     * (re-messaged {@link ShuffleBufferExceededException}). All counter mutations on the spill path
     * stay under {@link #admitLock} (same accounting path as {@code releaseLocked}); the byte budget
     * then tracks ONLY the in-memory-resident bytes. {@link #spilledTotalBytes} is the node-wide
     * on-disk total, checked against {@link #spillMaxBytes}.
     */
    private volatile boolean spillEnabled = false;
    private volatile Path spillDir = null;
    private volatile long spillMaxBytes = Long.MAX_VALUE;
    private final AtomicLong spilledTotalBytes = new AtomicLong();

    /**
     * Spill files whose buffer-level {@code deleteSpillFiles} FAILED to delete (disk/IO error): their
     * {@link #spilledTotalBytes} bytes stay charged (the file still occupies disk), and the owning
     * {@code SpilledSide} is dropped, so the only record of how much is charged for that path lives
     * here. The per-query dir sweep ({@link #deleteQuerySpillDir}) consults this map: when it succeeds
     * in deleting an orphaned path, it releases exactly those bytes (once) — otherwise a transient
     * delete failure would permanently inflate {@code spilledTotalBytes} → a false {@code spill.max_bytes}
     * ceiling until JVM restart. (codex review round-4 SHOULD-FIX #2.)
     */
    private final Map<Path, Long> orphanedSpillBytes = new ConcurrentHashMap<>();

    /**
     * Per-chunk on-disk framing overhead: each spilled chunk is written as a big-endian 4-byte length
     * prefix followed by its payload (see {@code SpilledSide.append}). The disk-budget accounting
     * ({@link #reserveSpillBytes}, {@code SpilledSide.bytesOnDisk}, release) must include this header,
     * else many small chunks let real disk usage exceed {@link #spillMaxBytes} by {@code 4 × chunkCount}
     * while the counter stays under the ceiling. The on-HEAP eviction target tracks only payload bytes
     * (the prefix is never heap-resident), so {@code spillOldest}'s returned {@code evicted} stays raw.
     */
    private static final int SPILL_FRAME_HEADER_BYTES = Integer.BYTES;
    // Admission (check-then-reserve) is serialized so concurrent producers can't both pass the
    // budget check and over-commit. Scope is the counter check+increment ONLY; storage and drain
    // stay outside the lock. Chunk arrival rate is modest (transport RPCs), so this is not hot.
    private final Object admitLock = new Object();

    /** Outcome of {@link #tryAdmit}. */
    public enum AdmitResult {
        /** Accepted; counters reserved and the chunk stored. */
        ACCEPTED,
        /** Node over budget but this query still fits its share — producer should back off + retry. */
        REJECT_RETRY
    }

    /**
     * Tombstones for queries that have been {@link #clearForQuery cleared} (terminated / aborted),
     * keyed {@code queryId → expiry nanoTime}. A producer RPC for a cancelled query can race in AFTER
     * its buffers were cleared and would otherwise re-create a buffer (via {@link #tryAdmit} or
     * {@link #getOrCreateBuffer}) that no consumer will ever drain/remove → a fresh leak, AND reserve
     * budget that no release would reclaim. While a query is tombstoned, admission drops the payload
     * without reserving and {@code getOrCreateBuffer} hands back a NON-stored throwaway buffer.
     *
     * <p><b>Evicted by TTL, not count.</b> The protection a tombstone must provide is: outlive every
     * late producer RPC for that query. A late RPC's lifetime is bounded by the sender retry window
     * ({@link ShuffleSenderRetry}: 8 attempts, backoff capped at 5s, ≈ 6.4s of backoff + transport
     * RTTs ≈ 15s worst case). {@link #TOMBSTONE_TTL_NANOS} (120s) is an ~8× margin over that, so the
     * tombstone is guaranteed present whenever a late RPC can still arrive. A COUNT-based FIFO cap
     * (the prior design) is unsafe: under a high query rate, N other clears can evict a still-in
     * -window tombstone within the 15s producer window, re-opening the recreate-and-leak race (codex
     * review: tombstoned-admit-budget-leak via eviction). TTL eviction is also self-bounding in memory
     * (live tombstones ≈ clear-rate × TTL) and needs no separate ordering structure or lock — the
     * concurrent map's per-key atomicity suffices; expired entries are purged opportunistically in
     * {@code clearForQuery}.
     */
    private final Map<String, Long> aborted = new ConcurrentHashMap<>();
    private static final long TOMBSTONE_TTL_NANOS = TimeUnit.SECONDS.toNanos(120);

    /** True if {@code queryId} has a live (non-expired) tombstone. Lock-free; safe on the producer
     *  hot path. An expired tombstone (cleared > TTL ago, so no late RPC can still be in flight) reads
     *  as not-tombstoned and is reclaimed by the next {@code clearForQuery} purge. */
    private boolean isTombstoned(String queryId) {
        if (queryId == null) {
            return false;
        }
        Long expiry = aborted.get(queryId);
        // expiry - now > 0 (not now < expiry) to be robust to nanoTime wraparound.
        return expiry != null && expiry - System.nanoTime() > 0;
    }

    @Override
    public ShuffleBufferAccess getOrCreate(String queryId, int targetStageId, int partitionIndex) {
        return getOrCreateBuffer(queryId, targetStageId, partitionIndex);
    }

    /**
     * Configure the node-level shuffle budget. {@code nodeBudget} caps the total on-heap shuffle
     * bytes across ALL queries; {@code perQueryMax} caps any single query's footprint (a query
     * exceeding it fails fast and non-retryably). Pass {@code Long.MAX_VALUE} to disable a bound.
     */
    public void setBudgets(long nodeBudget, long perQueryMax) {
        this.nodeBudgetBytes = nodeBudget;
        this.perQueryMaxBytes = perQueryMax;
    }

    /**
     * Sets the per-slot in-flight window (see {@link #streamWindowBytes}). {@code Long.MAX_VALUE}
     * disables it, restoring accumulate-until-budget behaviour.
     */
    public void setStreamWindowBytes(long windowBytes) {
        this.streamWindowBytes = windowBytes <= 0 ? Long.MAX_VALUE : windowBytes;
    }

    /** Current per-slot in-flight window in bytes (for observability/tests). */
    public long getStreamWindowBytes() {
        return streamWindowBytes;
    }

    /**
     * Configure hash-shuffle disk spill. {@code enabled} is the master switch; {@code directory} is
     * the root under which per-query spill subdirs are written ({@code <directory>/<queryId>/});
     * {@code maxBytes} is the hard node-wide disk ceiling (exceeding it fails the query). When
     * disabled, the per-query budget breach in {@link #tryAdmit} stays the fail-fast throw (behavior
     * byte-identical to the no-spill path). Called at plugin startup and on dynamic updates.
     */
    public void setSpillConfig(boolean enabled, Path directory, long maxBytes) {
        this.spillEnabled = enabled;
        this.spillDir = directory;
        this.spillMaxBytes = maxBytes;
    }

    /** True when disk spill is enabled (for observability/tests). */
    public boolean isSpillEnabled() {
        return spillEnabled;
    }

    /** Node-wide bytes currently written to spill files across all buffers (for observability/tests). */
    public long getSpilledTotalBytes() {
        return spilledTotalBytes.get();
    }

    public long getNodeBudgetBytes() {
        return nodeBudgetBytes;
    }

    public long getPerQueryMaxBytes() {
        return perQueryMaxBytes;
    }

    /** Current total on-heap shuffle bytes reserved across all buffers (for observability/tests). */
    public long getTotalBytes() {
        return totalBytes.get();
    }

    /** Bytes currently reserved for {@code queryId} (0 if no reservation / fully released). For
     *  observability + tests asserting per-query footprint cleanup, not just the node total. */
    public long getQueryBytes(String queryId) {
        AtomicLong q = perQueryBytes.get(queryId);
        return q == null ? 0L : q.get();
    }

    /** Number of live (non-expired) tombstones. Test/observability hook for the TTL eviction. */
    public int liveTombstoneCount() {
        long now = System.nanoTime();
        int n = 0;
        for (Long expiry : aborted.values()) {
            if (expiry - now > 0) {
                n++;
            }
        }
        return n;
    }

    /**
     * Admits {@code size} bytes for {@code (queryId,side)} against the node + per-query budgets, and
     * — on {@link AdmitResult#ACCEPTED} — stores {@code data} into the buffer and reserves the bytes.
     *
     * <ul>
     *   <li>throws {@link ShuffleBufferExceededException} if the query's OWN footprint would exceed
     *       {@code perQueryMaxBytes} (never fits — fail fast, non-retryable);</li>
     *   <li>returns {@link AdmitResult#REJECT_RETRY} if the NODE total would exceed
     *       {@code nodeBudgetBytes} while the query still fits its share (transient contention —
     *       back off and retry, room frees when other queries finish);</li>
     *   <li>returns {@link AdmitResult#ACCEPTED} otherwise (reserved + stored).</li>
     * </ul>
     * Buffer resolution, check-and-reserve, and storage are ALL atomic under {@link #admitLock}.
     */
    public AdmitResult tryAdmit(String queryId, int targetStageId, int partitionIndex, String side, byte[] data) {
        int size = data == null ? 0 : data.length;
        if (size == 0) {
            return AdmitResult.ACCEPTED; // nothing to store/reserve (isLast markers carry no data)
        }
        // Resolve the buffer, reserve the bytes, AND store the chunk together under admitLock, keyed
        // on a SINGLE tombstone read. Doing the buffer lookup lock-free (the old getOrCreateBuffer
        // call) BEFORE the locked re-check left a leak: getOrCreateBuffer could hand back an unstored
        // throwaway for a tombstoned query, the tombstone could then be evicted, and the locked
        // re-check would see a clean set and reserve bytes into that throwaway — which no removeBuffer
        // /clearForQuery would ever release (codex review: tombstoned-admit-budget-leak via eviction).
        //
        // No-leak proof vs a concurrent clearForQuery (which does aborted.put on a ConcurrentHashMap,
        // NOT under admitLock, then sweeps + releases UNDER admitLock):
        // - If the put has linearized before our isTombstoned read: we see tombstoned, bail, reserve
        // nothing. Safe.
        // - If the put has NOT yet linearized: we miss it, reserve + store under admitLock, then
        // release the lock. clearForQuery's sweep can only ENTER admitLock after we exit, so it is
        // ordered strictly after our store and its perQueryBytes.remove + buffers sweep releases
        // exactly what we reserved. Safe. (The locked sweep — not the tombstone — is what reclaims
        // a reservation made in the put-not-yet-visible window; the tombstone only short-circuits
        // the EARLIER admits so the common case never stores into a doomed buffer.)
        synchronized (admitLock) {
            if (isTombstoned(queryId)) {
                // Cleared query: drop the payload, reserve nothing, store nothing. Ack rather than
                // REJECT_RETRY — the producer must not retry a cancelled query.
                return AdmitResult.ACCEPTED;
            }
            ShuffleBuffer buffer = buffers.computeIfAbsent(
                key(queryId, targetStageId, partitionIndex),
                k -> newBuffer(queryId, targetStageId, partitionIndex)
            );
            // Defensive: NEVER append data to a slot whose EOF sentinel has been enqueued — the
            // consumer terminates its stream at the sentinel, so this chunk would be silently dropped
            // (lost rows). The producer's two-phase close (DatafusionPartitionedSink: all data sends
            // before any isLast) makes this unreachable in the normal path, but a buggy/reordered late
            // RPC must FAIL LOUD here rather than under-deliver. (codex round-5 BLOCKER #2.)
            //
            // This REPLACES the old `buffer.isDraining()` form. Under pipelined shuffle add and drain
            // are concurrent BY DESIGN — `draining` is set as soon as the consumer's handler runs, long
            // before producers finish — so "is draining" no longer means "too late". EOF does: it fires
            // only once every declared sender on that slot has reported isLast.
            if (buffer.isEofEnqueued(ShuffleSlots.validate(side))) {
                buffer.recordRejected();
                // Poison the CONSUMER as well as failing this producer. The producer-side throw can lose
                // the race — the consumer may already have terminated at the premature EOF and produced
                // output — so the only way truncation is never silent is to fail both ends.
                buffer.failSlot(side);
                throw new IllegalStateException(
                    "Shuffle data ("
                        + size
                        + " bytes) admitted after end-of-stream on buffer "
                        + key(queryId, targetStageId, partitionIndex)
                        + " side="
                        + side
                        + " — the consumer has already terminated this slot's stream, so the chunk would be lost. "
                        + "This indicates a producer shipped data after its isLast (a close-ordering bug)."
                );
            }
            // In-flight window full: the consumer has not drained this slot fast enough. Soft, retryable
            // — room frees as the drain advances. This is the pacing signal that keeps residency bounded.
            if (buffer.queuedBytes(ShuffleSlots.validate(side)) + size > streamWindowBytes) {
                buffer.recordRejected();
                return AdmitResult.REJECT_RETRY;
            }
            AtomicLong q = perQueryBytes.computeIfAbsent(queryId, k -> new AtomicLong());
            long qProjected = q.get() + size;
            if (qProjected > perQueryMaxBytes) {
                if (spillEnabled) {
                    // Spill enabled: instead of failing fast, evict oldest in-memory chunks of this
                    // buffer to disk to make room for the incoming chunk, then store it. Spilling
                    // releases the evicted chunks' RESERVED bytes (totalBytes + perQueryBytes +
                    // buffer.currentBytes) via the SAME releaseLocked accounting path removeBuffer
                    // uses — perQueryBytes stays the single source of truth, no double-release. The
                    // byte budget then tracks only the in-memory-resident bytes. Only the DISK ceiling
                    // (spill.max_bytes / a write I/O error) is terminal — re-messaged below.
                    long bytesToFree = qProjected - perQueryMaxBytes;
                    long freed = spillToMakeRoom(buffer, queryId, targetStageId, side, bytesToFree);
                    // spillToMakeRoom already released each spilled buffer's own currentBytes; here we
                    // release the query + node aggregate for the grand total freed (single source of
                    // truth = perQueryBytes). No buffer.releaseCurrentBytes here — that would double-
                    // release (the per-buffer release happens inside spillToMakeRoom).
                    releaseLocked(queryId, freed);
                    // After eviction the query's resident footprint is q.get() (already decremented).
                    // If spill couldn't free enough — (a) a SINGLE chunk larger than the budget with
                    // nothing else resident (can't split), or (b) the footprint is held by same-stage
                    // siblings that are already DRAINING and thus skipped by spillToMakeRoom — then this
                    // chunk would push past perQueryMaxBytes. Case (a) is accepted (resident bounded by
                    // the largest single chunk). Case (b) is caught by the node-budget check just below:
                    // with node==perQuery the un-freed resident leaves no node headroom, so the admit
                    // returns REJECT_RETRY and the producer retries — succeeding once the draining
                    // sibling's removeBuffer frees its budget. Self-correcting, never over-commits heap
                    // against a draining buffer. (codex review round-4 BLOCKER #1.)
                } else {
                    buffer.recordRejected();
                    // Hard: this query alone can't fit — waiting never helps. Fail fast, non-retryable.
                    throw new ShuffleBufferExceededException(qProjected, perQueryMaxBytes);
                }
            }
            if (totalBytes.get() + size > nodeBudgetBytes) {
                buffer.recordRejected();
                // Soft: node is momentarily full but this query fits its share — retry later.
                return AdmitResult.REJECT_RETRY;
            }
            totalBytes.addAndGet(size);
            q.addAndGet(size);
            // Store under the lock so the buffer's currentBytes stays EXACTLY equal to its reserved
            // bytes — removeBuffer then releases precisely what was reserved, with no transient skew.
            buffer.addData(side, data);
        }
        return AdmitResult.ACCEPTED;
    }

    /**
     * Creates a stored buffer for {@code (queryId,targetStageId,partitionIndex)} and, when spill is
     * enabled, hands it the spill identity (root dir + key components) so it can lazily open per-side
     * spill files on demand. Throwaway buffers (the tombstone path) are created via the bare ctor and
     * never spill.
     */
    private ShuffleBuffer newBuffer(String queryId, int targetStageId, int partitionIndex) {
        ShuffleBuffer buffer = new ShuffleBuffer();
        if (spillEnabled && spillDir != null) {
            buffer.enableSpill(this, spillDir, queryId, targetStageId, partitionIndex);
        }
        return buffer;
    }

    /**
     * Evicts oldest in-memory chunks of {@code side} (then the other side if {@code side} alone can't
     * cover it) from {@code buffer} to its spill file until at least {@code targetBytes} have been
     * freed, and returns the bytes actually freed. MUST run with {@link #admitLock} held: it mutates
     * the buffer's in-memory lists and the caller releases the freed bytes from the budget counters
     * under the same lock, so the byte accounting stays consistent (no admit/release interleaving can
     * observe a half-spilled state). Before each chunk is written, the node-wide disk ceiling
     * {@link #spillMaxBytes} is checked; exceeding it — or any write I/O error — throws a re-messaged
     * {@link ShuffleBufferExceededException} naming the disk ceiling (the new terminal failure when
     * spill is enabled).
     */
    private long spillToMakeRoom(ShuffleBuffer buffer, String queryId, int targetStageId, String side, long targetBytes) {
        // NEVER spill a buffer a consumer has begun draining: its drain may have already snapshotted the
        // in-memory tail or opened the spill file for read, so mutating its lists/spill file here would
        // drop/duplicate rows or NPE in SpilledSide.append. `draining` is flipped under admitLock
        // (beginDrain), which we hold here, so these reads can't race a half-started drain. A draining
        // buffer also no longer accepts evictions, so it simply contributes nothing to `freed`. (codex
        // review round-4 BLOCKER #1: cross-partition spill could race a draining sibling.)
        long freed = 0L;
        // 1. Spill the RECEIVING buffer first — its incoming-side chunks, then its other side. Each
        // spillOldest releases the spilled bytes from THAT buffer's own currentBytes so removeBuffer
        // later releases only what's still resident (no double-release). (A receiving buffer that is
        // already draining — a retried/reordered admit landing post-drain — is skipped here too.)
        if (!buffer.isDraining()) {
            freed += spillBufferAllSlots(buffer, side, targetBytes);
        }
        if (freed >= targetBytes) {
            return freed;
        }
        // 2. The per-query budget is query-WIDE (q.get() across every partition on this node), but a
        // single partition buffer may not hold enough resident bytes to cover the breach when the
        // footprint is spread across sibling partitions of the SAME stage. Spill those (non-draining)
        // siblings too. Scoping to the same (queryId, stageId): buffers of a DIFFERENT (already-run)
        // stage are deliberately NOT touched — one could be draining. (round-3 BLOCKER #1.)
        String stagePrefix = queryId + ":" + targetStageId + ":";
        for (Map.Entry<String, ShuffleBuffer> e : buffers.entrySet()) {
            if (freed >= targetBytes) {
                break;
            }
            ShuffleBuffer sibling = e.getValue();
            if (sibling == buffer || sibling.isDraining() || !e.getKey().startsWith(stagePrefix)) {
                continue;
            }
            freed += spillBufferAllSlots(sibling, side, targetBytes - freed);
        }
        return freed;
    }

    /**
     * Spills {@code buffer}'s oldest resident chunks (the incoming {@code slot} first, then its other
     * slots) until at least {@code targetBytes} are freed or the buffer runs dry, releasing the spilled
     * bytes from this buffer's own {@code currentBytes}. Returns the bytes freed from THIS buffer. Every
     * slot's chunks are read back in arrival order at drain (each slot drains into its own stream, so
     * cross-slot interleaving is irrelevant). MUST run under {@link #admitLock}.
     */
    private long spillBufferAllSlots(ShuffleBuffer buffer, String slot, long targetBytes) {
        long freed = buffer.spillOldest(slot, targetBytes);
        for (String other : buffer.getSlots()) {
            if (freed >= targetBytes) {
                break;
            }
            if (other.equals(slot)) {
                continue; // already spilled above
            }
            freed += buffer.spillOldest(other, targetBytes - freed);
        }
        buffer.releaseCurrentBytes(freed);
        return freed;
    }

    /**
     * Reserves {@code size} bytes of node-wide spill (disk) budget. Returns true if it fits under
     * {@link #spillMaxBytes}; false otherwise (caller turns that into the terminal disk-ceiling
     * failure). Called from {@link ShuffleBuffer#spillOldest} before each chunk is written so the
     * on-disk total can't silently grow past the ceiling.
     */
    private boolean reserveSpillBytes(long size) {
        // No lock needed for the ceiling check itself — admission already serializes via admitLock,
        // and spilledTotalBytes is only ever incremented here (under admitLock) and decremented in
        // releaseSpillBytes (cleanup), so the read-modify-write is safe under the enclosing lock.
        long projected = spilledTotalBytes.get() + size;
        if (projected > spillMaxBytes) {
            return false;
        }
        spilledTotalBytes.addAndGet(size);
        return true;
    }

    /** Returns {@code size} bytes to the node-wide spill (disk) budget when a spill file is deleted. */
    private void releaseSpillBytes(long size) {
        if (size > 0) {
            spilledTotalBytes.accumulateAndGet(size, (cur, b) -> Math.max(0, cur - b));
        }
    }

    /**
     * Records {@code bytes} charged for a spill file at {@code path} whose buffer-level delete FAILED,
     * so {@link #deleteQuerySpillDir} can release them when it (re)deletes the file. Race-safe vs a
     * concurrent dir-sweep that may delete the file between our failed delete and this record: the
     * {@code Files.exists} check runs INSIDE the atomic {@code compute}, so if the sweep already removed
     * the file (its {@code remove(path)} returned null because we hadn't recorded yet), we release the
     * bytes here and store nothing instead of leaving a phantom orphan entry that never gets swept →
     * permanent {@code spilledTotalBytes} inflation. (codex round-5 SHOULD-FIX #3.)
     */
    private void recordOrphanedSpill(Path path, long bytes) {
        if (bytes <= 0) {
            return;
        }
        orphanedSpillBytes.compute(path, (p, existing) -> {
            if (Files.exists(p) == false) {
                // The file is already gone — a concurrent sweep beat our record. Release now; the
                // sweep's own remove() saw no entry, so we own the release. Drop the map entry.
                releaseSpillBytes(bytes + (existing == null ? 0L : existing));
                return null;
            }
            return (existing == null ? 0L : existing) + bytes;
        });
    }

    public ShuffleBuffer getOrCreateBuffer(String queryId, int targetStageId, int partitionIndex) {
        // If the query was already cleared (cancelled/terminated), do NOT repopulate the map — a late
        // producer RPC racing after clearForQuery would otherwise leave an undrained buffer behind.
        // Hand back an unstored throwaway so the write lands on a collectible object. (This path is
        // the isLast/senderDone case — it creates a 0-byte buffer and reserves no budget.)
        if (isTombstoned(queryId)) {
            return new ShuffleBuffer();
        }
        String k = key(queryId, targetStageId, partitionIndex);
        // Create via newBuffer so the buffer is spill-capable regardless of WHICH path creates it
        // first (a senderDone/isLast RPC can land before the first data chunk). newBuffer only
        // attaches the spill identity (lazy — no file is opened until spillOldest fires), so a
        // 0-byte buffer created here never touches disk. This must match tryAdmit's creation path,
        // else a bare buffer created here would silently disable spill for that partition.
        ShuffleBuffer buffer = buffers.computeIfAbsent(k, kk -> newBuffer(queryId, targetStageId, partitionIndex));
        // Double-check AFTER inserting: the tombstone-then-check above is not atomic with the
        // computeIfAbsent, so a producer that read not-tombstoned can still insert a key AFTER
        // clearForQuery's removal sweep already passed it. clearForQuery sets the tombstone BEFORE
        // sweeping, so any such late insert is guaranteed to observe the tombstone here — remove our
        // own entry and return a throwaway, closing the recreate race. (codex review: tombstone race.)
        if (isTombstoned(queryId)) {
            discardRacedBuffer(queryId, k, buffer);
            return new ShuffleBuffer();
        }
        return buffer;
    }

    /**
     * Removes a buffer that was created/returned by {@link #getOrCreateBuffer}'s {@code computeIfAbsent}
     * just as its query was tombstoned, releasing BOTH its on-heap resident bytes and its on-disk spill
     * bytes under {@link #admitLock} — exactly like {@link #removeBuffer}. {@code computeIfAbsent} may
     * have returned a PRE-EXISTING buffer that already admitted data and SPILLED; a lock-free
     * {@code buffers.remove} would skip releasing its spill bytes (released only by
     * {@code deleteSpillFiles}), so {@link #spilledTotalBytes} would stay permanently inflated → a false
     * spill ceiling for later queries. (codex review round-3 BLOCKER #2.) Package-private for a
     * deterministic unit test of the cleanup (the live branch only fires in a narrow concurrent window).
     */
    void discardRacedBuffer(String queryId, String k, ShuffleBuffer buffer) {
        ShuffleBuffer removed = null;
        synchronized (admitLock) {
            // Conditional remove(k, buffer): only remove if k still maps to OUR buffer. A racing
            // getOrCreateBuffer could have replaced the mapping; that thread then owns its buffer's
            // cleanup, so we must not remove (and mis-account) it.
            if (buffers.remove(k, buffer)) {
                removed = buffer;
                // Release resident bytes — normally 0 (this is the isLast path), but a pre-existing
                // buffer that admitted data carries real currentBytes.
                releaseLocked(queryId, removed.getCurrentBytes());
            }
        }
        if (removed != null) {
            // Wake any parked drain first: the buffer is out of the map, so nothing more will arrive.
            removed.abortStreams();
            removed.deleteSpillFiles(); // returns on-disk bytes to the node spill budget (I/O, off-lock)
        }
    }

    public ShuffleBuffer getBuffer(String queryId, int targetStageId, int partitionIndex) {
        return buffers.get(key(queryId, targetStageId, partitionIndex));
    }

    @Override
    public void removeBuffer(String queryId, int targetStageId, int partitionIndex) {
        // Map removal AND budget release happen together under admitLock so they're atomic with
        // admission and with clearForQuery. Only the caller that actually removed the entry (a
        // non-null buffers.remove) releases its bytes — a second remove of the same key, or a
        // concurrent clearForQuery that already swept it, sees null and releases nothing. This is
        // what makes per-buffer release (normal drain) and whole-query release (clearForQuery)
        // compose without double-subtracting (codex review: shuffle-budget-double-release).
        ShuffleBuffer removed;
        synchronized (admitLock) {
            removed = buffers.remove(key(queryId, targetStageId, partitionIndex));
            if (removed != null) {
                releaseLocked(queryId, removed.getCurrentBytes());
            }
        }
        // Delete this buffer's spill files (if any) OUTSIDE admitLock — file deletion is I/O and the
        // entry is already out of the map, so no admit/release can race it. deleteSpillFiles returns
        // the on-disk bytes it freed so the node-wide spill total is corrected; a leaked .spill file
        // is a disk leak (mirrors the in-memory cleanup). Only the caller that actually removed the
        // entry cleans up — a second remove / a concurrent clearForQuery sweep sees null and skips.
        if (removed != null) {
            removed.abortStreams();
            removed.deleteSpillFiles();
        }
    }

    /**
     * Subtracts {@code bytes} from the node total and the query's per-query footprint. MUST be called
     * with {@code admitLock} held — releases are serialized with admission and with each other so the
     * counters can't drift. {@code perQueryBytes} is the single source of truth for a query's
     * reservation; removeBuffer decrements it per drained buffer, clearForQuery removes whatever
     * remains, so the same bytes are never subtracted twice. Storage happens under admitLock in
     * tryAdmit, so a buffer's {@code currentBytes} exactly equals its reserved bytes — removeBuffer
     * releases precisely what was reserved. The max(0,...) floor is purely defensive.
     */
    private void releaseLocked(String queryId, long bytes) {
        assert Thread.holdsLock(admitLock) : "releaseLocked requires admitLock";
        if (bytes <= 0) {
            return;
        }
        totalBytes.accumulateAndGet(bytes, (cur, b) -> Math.max(0, cur - b));
        AtomicLong q = perQueryBytes.get(queryId);
        if (q != null) {
            q.accumulateAndGet(bytes, (cur, b) -> Math.max(0, cur - b));
        }
    }

    /**
     * Removes every buffer belonging to {@code queryId} (all stages, all partitions on this node).
     * Each {@link ShuffleBuffer} holds the query's shuffled payload as on-heap {@code byte[]} lists;
     * without this the entries live for the JVM's lifetime and accumulate across queries → on-heap
     * OOM. Invoked on EVERY terminal outcome of a query's data-node work — normal completion, error,
     * and task cancellation — so the on-heap payload becomes collectible as soon as the query ends.
     * Idempotent and safe to call when no buffers exist for the query (the common non-shuffle case).
     *
     * @return the number of buffers removed (for logging / test assertions)
     */
    @Override
    public int clearForQuery(String queryId) {
        if (queryId == null) {
            return 0;
        }
        // Tombstone the query FIRST (with a TTL expiry) so the COMMON case — a producer RPC racing in
        // after the clear (abort/cancel) — short-circuits in tryAdmit/getOrCreateBuffer and never
        // stores into a doomed buffer. The put is on a ConcurrentHashMap, NOT under admitLock; it is
        // done before the locked sweep below so that a producer whose isTombstoned read happens to
        // MISS the not-yet-visible put is still caught by the sweep (which can only enter admitLock
        // after that producer's reserve+store exits it — see the no-leak proof in tryAdmit). The TTL
        // must outlive any late producer RPC (sender retry window ≈ 15s); 120s is an ~8× margin. A
        // repeat clearForQuery (the cancellation-listener backstop firing after normal cleanup)
        // refreshes the expiry — harmless and keeps the window open if work is somehow still trickling
        // in.
        long now = System.nanoTime();
        aborted.put(queryId, now + TOMBSTONE_TTL_NANOS);
        // Opportunistically purge expired tombstones so the map is bounded by clear-rate × TTL rather
        // than growing unbounded. Cheap: a weakly-consistent iteration over a small concurrent map,
        // amortized across clears. (No separate timer/lock — per-key conditional remove is atomic.)
        for (var it = aborted.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Long> e = it.next();
            if (e.getValue() - now <= 0) {
                aborted.remove(e.getKey(), e.getValue());
            }
        }
        String prefix = queryId + ":";
        int removed = 0;
        // Collect swept buffers so their spill files can be deleted OUTSIDE admitLock (file I/O).
        List<ShuffleBuffer> swept = new ArrayList<>();
        // Sweep + budget reclaim under admitLock so they're atomic with admission and removeBuffer:
        // any admit that reserved bytes for this query did so under admitLock and released it before
        // this sweep could acquire the lock, so the sweep observes (and reclaims) every such buffer +
        // reservation; removeBuffer likewise can't be mid-release on one of these buffers. perQueryBytes
        // is the single source of truth — we drop its entry and subtract WHATEVER REMAINS (already net
        // of any prior per-buffer removeBuffer decrements), so the same bytes are never subtracted
        // twice (codex review: shuffle-budget-double-release).
        synchronized (admitLock) {
            // Iterate the entry set and remove matching entries. ConcurrentHashMap's iterator is weakly
            // consistent — safe to remove through it; concurrent producer/consumer activity for a still
            // -running query won't be cleared because only this query's terminal hook calls clearForQuery.
            for (var it = buffers.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, ShuffleBuffer> entry = it.next();
                if (entry.getKey().startsWith(prefix)) {
                    swept.add(entry.getValue());
                    it.remove();
                    removed++;
                }
            }
            // Release the whole query's reserved footprint in one shot (drop its perQueryBytes entry
            // and subtract it from the node total), so a cancelled/failed query's budget is fully
            // reclaimed.
            AtomicLong q = perQueryBytes.remove(queryId);
            if (q != null) {
                long freed = q.getAndSet(0);
                if (freed > 0) {
                    totalBytes.accumulateAndGet(freed, (cur, b) -> Math.max(0, cur - b));
                }
            }
        }
        // Delete spill files for every swept buffer + the per-query spill subdir, OUTSIDE admitLock.
        // A leaked .spill file (or subdir) is a disk leak — this mirrors the in-memory cleanup on
        // EVERY terminal (success/fail/cancel). The buffers are already out of the map, so no admit
        // can recreate the on-disk state for this (now tombstoned) query.
        for (ShuffleBuffer b : swept) {
            b.abortStreams();
            b.deleteSpillFiles();
        }
        deleteQuerySpillDir(queryId);
        if (removed > 0) {
            LOGGER.debug("Cleared {} shuffle buffer(s) for query {} (freed budget)", removed, queryId);
        }
        return removed;
    }

    private static String key(String queryId, int targetStageId, int partitionIndex) {
        return queryId + ":" + targetStageId + ":" + partitionIndex;
    }

    /**
     * Sanitizes {@code queryId} into a single safe path segment for the spill subdir name. Query ids
     * are engine-generated UUID-like strings, but defensively strip anything that isn't an
     * alphanumeric, dash, underscore or dot so a hostile / odd id can't escape {@link #spillDir} via
     * separators or {@code ..}.
     */
    private static String safeSegment(String queryId) {
        return queryId.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    /**
     * Deletes the per-query spill subdir ({@code <spillDir>/<queryId>/}) and everything under it,
     * best-effort. Called on every terminal from {@link #clearForQuery} after the per-buffer files
     * are deleted, so even a buffer that was swept without its own {@code deleteSpillFiles} running
     * (e.g. a throwaway path) can't leave an orphaned file behind. A no-op when spill is disabled or
     * the subdir never existed.
     */
    private void deleteQuerySpillDir(String queryId) {
        Path dir = spillDir;
        if (dir == null || queryId == null) {
            return;
        }
        Path queryDir = dir.resolve(safeSegment(queryId));
        if (!Files.isDirectory(queryDir)) {
            return;
        }
        try (var walk = Files.walk(queryDir)) {
            // Reverse order so children are deleted before their parent directory.
            walk.sorted(Collections.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                    // If this path was an orphan (a buffer-level delete failed earlier and its bytes are
                    // still charged), the dir sweep just reclaimed the disk — release those bytes exactly
                    // once. Use compute (not remove) so this is atomic vs a concurrent recordOrphanedSpill
                    // on the same path: whichever runs first removes the entry + releases; the other sees
                    // null and does nothing. (round-4 #2 / codex round-5 SHOULD-FIX #3.)
                    orphanedSpillBytes.compute(p, (path, orphanBytes) -> {
                        if (orphanBytes != null) {
                            releaseSpillBytes(orphanBytes);
                        }
                        return null; // drop the entry either way — the file is gone
                    });
                } catch (IOException e) {
                    LOGGER.debug(new ParameterizedMessage("Failed to delete shuffle spill path {} for query {}", p, queryId), e);
                }
            });
        } catch (IOException e) {
            LOGGER.debug(new ParameterizedMessage("Failed to walk shuffle spill dir {} for query {}", queryDir, queryId), e);
        }
    }

    /**
     * Per-partition shuffle buffer — a store + completion latches. Thread-safe: multiple senders
     * call {@link #addData} concurrently (after the manager's {@link #tryAdmit} reserves budget);
     * one consumer thread drains via {@link #getLeftData} / {@link #getRightData} after
     * {@link #awaitReady} returns. Byte-budget enforcement lives in the enclosing manager, not here.
     */
    public static class ShuffleBuffer implements ShuffleBufferAccess {

        /**
         * Per-slot accumulation + completion state. One entry per input stream the consumer will
         * read (see {@link ShuffleSlots}): a binary hash join has {@code left} + {@code right}, a
         * FINAL-aggregate worker has only {@code left}, and an N-way worker tier has one per input.
         *
         * <p>Created on demand by whichever thread first touches the slot ({@code addData} from a
         * producer RPC, {@code senderDone} from an isLast, or {@code setExpectedSenders} from the
         * consumer's setup handler), so no slot list needs to be known up front.
         */
        private static final class Slot {
            /**
             * Bounded FIFO of admitted chunks, drained CONCURRENTLY by the consumer. This is the
             * pipelined-shuffle core: the old design was a {@code List} that accumulated the WHOLE
             * partition because {@code awaitReady} blocked the drain until every producer finished, so
             * peak residency scaled with partition size (on-heap {@code byte[]} AND again as Arrow
             * buffers at drain). With a bounded queue, residency is {@code capacity × chunk size} —
             * independent of partition size — and a full queue becomes producer backpressure.
             */
            private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
            private final AtomicInteger doneCount = new AtomicInteger();
            private final CountDownLatch ready = new CountDownLatch(1);
            private volatile int expectedSenders = -1;
            /**
             * Enqueued-EOF guard. The sentinel must be enqueued EXACTLY once (a second one would
             * truncate a sibling reader), and it is also the new "too late to add data" marker that
             * replaces the old {@code draining} check in {@link ShuffleBufferManager#tryAdmit}.
             */
            private final AtomicBoolean eofEnqueued = new AtomicBoolean();
            /** Chunks currently queued (not yet taken) — observability. */
            private final AtomicInteger queuedChunks = new AtomicInteger();
            /**
             * Bytes currently queued but not yet taken by the consumer — the IN-FLIGHT WINDOW. This is
             * what makes peak residency independent of partition size: admission rejects (retryably)
             * once a slot's window is full, so producers pace themselves to the consumer's drain rate
             * instead of accumulating the whole partition. Decremented when the drain takes a chunk or
             * the spill path evicts one.
             */
            private final AtomicLong queuedBytes = new AtomicLong();
            /**
             * Set when this slot's stream has been INVALIDATED (a late admit past end-of-stream, or a
             * terminal). Checked by the drain at its EOF transition, so it does not depend on queue
             * ORDER: the poison sentinel is necessarily enqueued after the EOF sentinel, and an in-order
             * consumer would otherwise terminate cleanly at EOF and never see it — which is exactly the
             * silent truncation this guards against.
             */
            private volatile boolean invalidated;
            /** On-disk file for this slot, lazily created on first spill (null when never spilled). */
            private SpilledSide spill;
        }

        /**
         * End-of-stream marker enqueued once per slot when every declared sender has reported
         * {@code isLast}. Compared by IDENTITY (never by content), so a real zero-length payload can
         * never be mistaken for EOF. Replaces the {@code ready} latch as the drain's terminator.
         */
        static final byte[] EOF_SENTINEL = new byte[0];

        /**
         * Poison marker enqueued on every slot when a buffer is removed or its query is cleared
         * (terminal / cancellation). Without it a drain parked in {@code queue.poll(timeout)} would sit
         * for the full liveness timeout after its query died, holding an Arrow allocator and a thread.
         *
         * <p>Deliberately DISTINCT from {@link #EOF_SENTINEL}: waking a consumer with a clean
         * end-of-stream would let it close the native sender normally, and the join would emit results
         * from a truncated input. This one makes the drain THROW, so the caller fails the native stream.
         */
        static final byte[] CANCELLED_SENTINEL = new byte[0];

        /**
         * Default per-chunk wait for a streaming drain. This is a LIVENESS bound, not a data bound:
         * the drain now blocks for the NEXT chunk rather than for the whole partition, so it only
         * trips when producers have genuinely stalled. Matches the old whole-partition
         * {@code awaitReady} budget so a slow-but-working shuffle behaves as before.
         */
        static final long DEFAULT_STREAM_POLL_TIMEOUT_MILLIS = 300_000L;

        /**
         * Slots by label. {@link ConcurrentHashMap} because producer RPCs (arbitrary transport
         * threads) and the consumer's handler chain create/read entries concurrently;
         * {@code computeIfAbsent} makes slot creation atomic so two producers racing on the same new
         * slot cannot install two accumulation lists (which would silently drop one's rows).
         */
        private final Map<String, Slot> slots = new ConcurrentHashMap<>();
        private final AtomicLong currentBytes = new AtomicLong();
        private final AtomicLong rejectedCount = new AtomicLong();

        /**
         * Spill state. Null/disabled by default — a buffer only spills when the manager wires its
         * spill identity via {@link #enableSpill}. Each {@link Slot#spill} is that slot's on-disk
         * file (lazily created on first spill). All spill state mutation happens under the manager's
         * {@code admitLock} (spill) or after the buffer is out of the map (drain/cleanup), so the
         * per-slot fields need no further synchronization.
         */
        private ShuffleBufferManager owner;
        private Path querySpillDir;
        private String spillKey; // <stageId>-<partition>

        /**
         * Set once a consumer begins draining this buffer (snapshotting the in-memory tail / opening
         * the spill file for read). Flipped under the manager's {@code admitLock} BEFORE the drain
         * touches any list/file, so {@code spillToMakeRoom} — which also runs under {@code admitLock} —
         * never spills a buffer that is concurrently draining. Without this, a late cross-partition
         * admit could spill a sibling whose drain iterator has already snapshotted its tail or closed
         * its append stream, dropping/duplicating rows or NPEing in {@code SpilledSide.append}.
         * (codex review round-4 BLOCKER #1.) volatile so the spill path sees a set made under the lock.
         */
        private volatile boolean draining;

        public ShuffleBuffer() {}

        /**
         * Wires this buffer's spill identity (lazy — no file is opened here). Once set, a per-query
         * breach in {@link ShuffleBufferManager#tryAdmit} evicts this buffer's oldest chunks to a
         * per-side spill file under {@code <queryDir>} instead of failing fast. Idempotent.
         */
        void enableSpill(ShuffleBufferManager owner, Path spillRoot, String queryId, int stageId, int partition) {
            this.owner = owner;
            this.querySpillDir = spillRoot.resolve(safeSegment(queryId));
            this.spillKey = stageId + "-" + partition;
        }

        @Override
        public void setExpectedSenders(Map<String, Integer> expectedSendersBySlot) {
            for (Map.Entry<String, Integer> e : expectedSendersBySlot.entrySet()) {
                Integer count = e.getValue();
                if (count == null || count < 0) {
                    continue; // negative/absent means "leave unchanged" (see the SPI contract)
                }
                String slotId = ShuffleSlots.validate(e.getKey());
                slotFor(slotId).expectedSenders = count;
                // A producer's isLast may already have arrived (it does not need the consumer's
                // count), so re-check completion now that the target is known.
                checkCompletion(slotId);
            }
        }

        /** The slot for {@code slotId}, created on first touch. */
        private Slot slotFor(String slotId) {
            return slots.computeIfAbsent(slotId, k -> new Slot());
        }

        /**
         * Stores {@code data} on the named slot and tracks this buffer's byte total. Admission /
         * budget enforcement is owned by the enclosing {@link ShuffleBufferManager#tryAdmit} (node +
         * per-query budgets); this method only stores after admission has reserved the bytes. The
         * tracked {@link #currentBytes} is read at removal time to release the reservation.
         */
        public void addData(String slot, byte[] data) {
            int size = data == null ? 0 : data.length;
            currentBytes.addAndGet(size);
            enqueue(slot, data);
        }

        /**
         * Publishes {@code data} to {@code slot}'s stream. Split out from the byte accounting so
         * {@link ShuffleBufferManager#tryAdmit} can reserve under {@code admitLock} and publish
         * OUTSIDE it — the queue is thread-safe and the consumer drains concurrently, so holding the
         * global admission lock across a publish would serialize every producer on the node.
         *
         * <p>Refuses after the slot's EOF sentinel (returns false). That is the streaming replacement
         * for the old "never append to a draining buffer" rule: with concurrent drain, {@code draining}
         * is set almost immediately and can no longer mean "too late", but data after EOF is still
         * unambiguously a producer close-ordering bug.
         */
        boolean enqueue(String slot, byte[] data) {
            Slot s = slotFor(ShuffleSlots.validate(slot));
            if (s.eofEnqueued.get()) {
                return false;
            }
            s.queuedChunks.incrementAndGet();
            s.queuedBytes.addAndGet(data == null ? 0 : data.length);
            s.queue.add(data);
            return true;
        }

        /** Chunks currently queued but not yet taken by the consumer, on {@code slot} (0 if unknown). */
        int queuedChunks(String slot) {
            Slot s = slots.get(slot);
            return s == null ? 0 : s.queuedChunks.get();
        }

        /** Bytes queued but not yet taken on {@code slot} — the in-flight window (0 if unknown). */
        long queuedBytes(String slot) {
            Slot s = slots.get(slot);
            return s == null ? 0L : s.queuedBytes.get();
        }

        /** True once {@code slot}'s EOF sentinel has been enqueued (all declared senders reported isLast). */
        boolean isEofEnqueued(String slot) {
            Slot s = slots.get(slot);
            return s != null && s.eofEnqueued.get();
        }

        /** Records a rejected/failed admission attempt against this buffer (observability). */
        public void recordRejected() {
            rejectedCount.incrementAndGet();
        }

        public long getCurrentBytes() {
            return currentBytes.get();
        }

        public long getRejectedCount() {
            return rejectedCount.get();
        }

        /**
         * Subtracts {@code bytes} from this buffer's resident byte counter after the manager has
         * spilled (and budget-released) those bytes. Keeps {@link #currentBytes} equal to the
         * IN-MEMORY-resident bytes, so a later {@link ShuffleBufferManager#removeBuffer} releases only
         * what is still resident — never the already-released spilled bytes (no double-release).
         */
        void releaseCurrentBytes(long bytes) {
            if (bytes > 0) {
                currentBytes.accumulateAndGet(bytes, (cur, b) -> Math.max(0, cur - b));
            }
        }

        /**
         * Appends the oldest in-memory chunks of {@code side} (length-prefixed) to that side's spill
         * file and removes them from the in-memory list until at least {@code targetBytes} have been
         * evicted; returns the bytes actually evicted (may be less than {@code targetBytes} if the
         * side runs out of resident chunks). Oldest-first (front of the list) so reading the spill
         * file back in write order, then the in-memory tail, reconstructs ARRIVAL order at drain.
         *
         * <p>Called under the manager's {@code admitLock}; the manager releases the returned bytes
         * from the budget counters (and this buffer's {@link #currentBytes}) under the same lock. A
         * single chunk is never split — if the oldest chunk alone is larger than {@code targetBytes}
         * it is still spilled whole (resident then drops below the budget). Throws a re-messaged
         * {@link ShuffleBufferExceededException} if the node-wide disk ceiling is hit or a write fails.
         */
        long spillOldest(String slotId, long targetBytes) {
            if (querySpillDir == null || targetBytes <= 0) {
                return 0L;
            }
            Slot slot = slots.get(slotId);
            if (slot == null) {
                return 0L; // nothing ever arrived on this slot
            }
            BlockingQueue<byte[]> queue = slot.queue;
            long evicted = 0L;
            while (evicted < targetBytes) {
                // Head-of-queue eviction, oldest first, so spill-file order stays arrival order. Safe
                // to peek-then-poll without extra locking: only producers append (at the tail), and
                // spillToMakeRoom runs under admitLock and SKIPS draining buffers, so no consumer is
                // concurrently taking from this head.
                byte[] head = queue.peek();
                if (head == null || head == EOF_SENTINEL) {
                    break; // nothing resident, or only the terminator is left — never spill the sentinel
                }
                int len = head.length;
                // Disk footprint includes the 4-byte frame header append() writes, so the on-disk
                // total can't silently grow past the ceiling by 4×chunkCount. (codex round-2.)
                long diskBytes = len + SPILL_FRAME_HEADER_BYTES;
                // Reserve disk budget BEFORE removing from memory — if the ceiling is hit we
                // leave the chunk resident and fail (it's still safely in memory/accounted).
                if (!owner.reserveSpillBytes(diskBytes)) {
                    throw ShuffleBufferExceededException.forDiskCeiling(owner.getSpilledTotalBytes() + diskBytes, owner.spillMaxBytes);
                }
                byte[] chunk = queue.poll();
                if (chunk == null || chunk == EOF_SENTINEL) {
                    // Raced with the terminator being enqueued: give the disk reservation back and, if
                    // we took the sentinel, restore it so the drain still terminates.
                    owner.releaseSpillBytes(diskBytes);
                    if (chunk == EOF_SENTINEL) {
                        queue.add(chunk);
                    }
                    break;
                }
                slot.queuedChunks.decrementAndGet();
                slot.queuedBytes.addAndGet(-len);
                try {
                    SpilledSide spill = spillFor(slotId, slot);
                    spill.append(chunk);
                } catch (IOException e) {
                    // The disk bytes for THIS chunk were reserved (reserveSpillBytes above) but the
                    // write failed, so they were never recorded in SpilledSide.bytesOnDisk() and the
                    // terminal cleanup (which releases bytesOnDisk) would NOT reclaim them → a
                    // permanent spilledTotalBytes leak that shrinks the node's effective spill
                    // ceiling for later queries. Release the reserved-but-unwritten bytes here.
                    // (codex review BLOCKER: reserved-but-not-written disk-byte leak.)
                    owner.releaseSpillBytes(diskBytes);
                    throw ShuffleBufferExceededException.forDiskCeiling(owner.getSpilledTotalBytes(), owner.spillMaxBytes);
                }
                // evicted tracks ON-HEAP bytes freed (the eviction target is the heap budget); the
                // frame header is disk-only, so it is intentionally excluded here.
                evicted += len;
            }
            return evicted;
        }

        /** Lazily opens (and remembers) the slot's spill file append stream. */
        private SpilledSide spillFor(String slotId, Slot slot) throws IOException {
            if (slot.spill == null) {
                slot.spill = new SpilledSide(spillFilePath(slotId));
            }
            return slot.spill;
        }

        /** Spill-file path for one slot. The slot label is validated on the way in
         *  ({@link ShuffleSlots#validate}), so it cannot escape {@link #querySpillDir}. */
        private Path spillFilePath(String slotId) {
            return querySpillDir.resolve(spillKey + "-" + slotId + ".spill");
        }

        /**
         * Deletes every slot's spill file (closing their streams first) and returns their on-disk
         * bytes to the manager's node-wide spill budget. Best-effort, idempotent; called on every
         * terminal (removeBuffer / clearForQuery). A leaked .spill file is a disk leak.
         */
        /**
         * Wakes any drain parked on this buffer and makes it fail. Called on every terminal path
         * (removeBuffer / clearForQuery) BEFORE the buffer's bytes are released, so a consumer can never
         * be left blocked on a queue whose owner has gone away. Idempotent and non-blocking: an extra
         * poison on an already-finished slot is simply never read.
         */
        void abortStreams() {
            for (Slot slot : slots.values()) {
                slot.invalidated = true;
                slot.queue.add(CANCELLED_SENTINEL);
                // Release anything waiting on the old latch too (tests / awaitReady callers).
                slot.ready.countDown();
            }
        }

        /**
         * Poisons ONE slot's stream so a live consumer fails instead of finishing on truncated input.
         *
         * <p>Safety net for a wrong {@code expectedSenders}. EOF is published when {@code doneCount}
         * reaches the declared sender count; if that count is too LOW, EOF fires while a straggling
         * producer still has rows to ship, the consumer terminates, and the join silently returns fewer
         * rows — a wrong answer with an HTTP 200 (measured: 18,215 late admits on one worker while the
         * query "succeeded"). The producer-side throw alone is not enough, because the consumer may
         * already have finished by the time it propagates.
         *
         * <p>So the CONSUMER is poisoned too: whichever side notices first, the query cannot return a
         * truncated result. Uses {@link #CANCELLED_SENTINEL}, not {@link #EOF_SENTINEL} — a clean
         * end-of-stream is exactly what must not happen here.
         */
        void failSlot(String slot) {
            Slot s = slots.get(ShuffleSlots.validate(slot));
            if (s != null) {
                s.invalidated = true;          // order-independent: seen even if EOF is already queued
                s.queue.add(CANCELLED_SENTINEL); // wakes a drain parked in poll()
                s.ready.countDown();
            }
        }

        void deleteSpillFiles() {
            for (Slot slot : slots.values()) {
                slot.spill = closeAndDelete(slot.spill);
            }
        }

        private SpilledSide closeAndDelete(SpilledSide spill) {
            if (spill == null) {
                return null;
            }
            long onDisk = spill.bytesOnDisk();
            // Release the disk-budget reservation ONLY if the file was actually removed (or was never
            // written). If deletion fails (disk/IO error) the bytes still occupy disk, so dropping the
            // counter would let later queries admit past spill.max_bytes — keep them charged instead.
            // (codex review round-3 SHOULD-FIX.)
            boolean deleted = spill.closeAndDelete();
            if (owner != null) {
                if (deleted) {
                    owner.releaseSpillBytes(onDisk);
                } else if (onDisk > 0) {
                    owner.recordOrphanedSpill(spill.path(), onDisk);
                }
            }
            return null;
        }

        public void senderDone(String slot) {
            String slotId = ShuffleSlots.validate(slot);
            slotFor(slotId).doneCount.incrementAndGet();
            checkCompletion(slotId);
        }

        private void checkCompletion(String slotId) {
            Slot slot = slots.get(slotId);
            if (slot == null) {
                return;
            }
            int expected = slot.expectedSenders;
            if (expected >= 0 && slot.doneCount.get() >= expected) {
                // Publish EOF exactly once, BEFORE releasing the latch: a consumer woken by the latch
                // must find the terminator already queued, never race ahead of it.
                if (slot.eofEnqueued.compareAndSet(false, true)) {
                    slot.queue.add(EOF_SENTINEL);
                }
                slot.ready.countDown();
            }
        }

        /**
         * Wait for every DECLARED slot's senders to complete. Returns {@code false} on timeout.
         *
         * <p>Waits only on slots whose expected count has been set: an undeclared slot (one that
         * exists solely because a producer's payload arrived early) has no target to compare against,
         * so waiting on it could never succeed. The consumer's setup handler declares all of its
         * slots in one call BEFORE any drain, which is what makes this set complete — see
         * {@link ShuffleBufferAccess#setExpectedSenders(Map)}.
         */
        public boolean awaitReady(long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            // Snapshot the slot set so a concurrently-created slot (an early producer payload for a
            // slot this consumer never declared) can't extend the wait mid-loop.
            for (Map.Entry<String, Slot> e : new ArrayList<>(slots.entrySet())) {
                Slot slot = e.getValue();
                if (slot.expectedSenders < 0) {
                    continue; // never declared by the consumer — nothing to wait for
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                if (!slot.ready.await(remaining, TimeUnit.MILLISECONDS)) {
                    LOGGER.warn(
                        "Shuffle slot {} timed out: received {}/{} senders",
                        e.getKey(),
                        slot.doneCount.get(),
                        slot.expectedSenders
                    );
                    return false;
                }
            }
            return true;
        }

        /**
         * Marks this buffer as draining so the spill path stops touching it. Flips {@link #draining}
         * under the owner's {@code admitLock} (when an owner is wired) so the set is ordered against
         * {@code spillToMakeRoom}: once this returns, no concurrent or subsequent spill will evict from
         * this buffer (the sibling-spill loop skips {@code draining} buffers). Called at the head of
         * every drain entry point, BEFORE any list snapshot or spill-file open. A throwaway buffer
         * (no owner) just sets the flag. (codex review round-4 BLOCKER #1.)
         */
        private void beginDrain() {
            if (owner != null) {
                synchronized (owner.admitLock) {
                    draining = true;
                }
            } else {
                draining = true;
            }
        }

        /** True once a consumer has begun draining this buffer (see {@link #draining}). */
        boolean isDraining() {
            return draining;
        }

        @Override
        public List<byte[]> getData(String slot) {
            // EAGER, NON-BLOCKING form: returns what is resident RIGHT NOW (spilled chunks, then the
            // currently-queued ones) and stops at the queue's end or the EOF sentinel. It must not wait
            // for EOF: callers use this to inspect what has accumulated so far, with no guarantee that
            // any sender has reported isLast, so blocking would hang them. The streaming consumer path
            // uses drain(slot, timeout) instead.
            beginDrain();
            Slot s = slots.get(ShuffleSlots.validate(slot));
            if (s == null) {
                return List.of(); // nothing ever arrived on this slot
            }
            List<byte[]> out = new ArrayList<>();
            if (s.spill != null) {
                try {
                    out.addAll(s.spill.readBack());
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to read back shuffle spill file " + s.spill.path(), e);
                }
            }
            while (true) {
                byte[] chunk = s.queue.poll();
                if (chunk == null) {
                    break;
                }
                if (chunk == EOF_SENTINEL) {
                    // Put the terminator back so a subsequent streaming drain still terminates.
                    s.queue.add(chunk);
                    break;
                }
                s.queuedChunks.decrementAndGet();
                s.queuedBytes.addAndGet(-chunk.length);
                out.add(chunk);
            }
            return out;
        }

        @Override
        public CloseableIterator<byte[]> drain(String slot) {
            // NON-BLOCKING, and deliberately so: this is the pre-streaming SPI method, whose contract is
            // "lazily yield what is resident". Callers of this form (tests, small non-streaming callers)
            // do not necessarily complete their senders, so waiting for EOF here would hang them. The
            // streaming consumer opts in explicitly via drain(slot, timeoutMillis).
            beginDrain();
            Slot s = slotFor(ShuffleSlots.validate(slot));
            return new StreamingSlotIterator(s, 0L, slot);
        }

        @Override
        public CloseableIterator<byte[]> drain(String slot, long timeoutMillis) {
            beginDrain();
            // slotFor (not slots.get): the consumer can now start draining BEFORE any producer has
            // touched this slot, so the slot may not exist yet. Creating it here is what lets the
            // stream block for the first chunk instead of mis-reporting an empty partition.
            Slot s = slotFor(ShuffleSlots.validate(slot));
            return new StreamingSlotIterator(s, timeoutMillis, slot);
        }

        /**
         * STREAMING drain: yields any already-spilled chunks (arrival order, one at a time off disk),
         * then blocks for live chunks off the slot's queue until the EOF sentinel.
         *
         * <p>This replaces the old snapshot-based drain. The difference that matters: the old iterator
         * was created AFTER {@code awaitReady} guaranteed the partition was complete, so it could
         * safely copy the in-memory list; this one runs CONCURRENTLY with producers, so it must consume
         * the live queue and rely on the sentinel to know when to stop. That is what removes
         * partition-sized residency — nothing accumulates waiting for a barrier.
         *
         * <p>Terminates on: EOF sentinel (clean), poll timeout (producers stalled → throw, never a
         * silent short read), or interrupt (cancellation).
         */
        private static final class StreamingSlotIterator implements CloseableIterator<byte[]> {
            private final Slot slot;
            private final long timeoutMillis;
            private final String slotLabel;
            private Iterator<byte[]> spilled;
            private byte[] pending;
            private boolean eof;

            StreamingSlotIterator(Slot slot, long timeoutMillis, String slotLabel) {
                this.slot = slot;
                this.timeoutMillis = timeoutMillis;
                this.slotLabel = slotLabel;
                // Spilled chunks were written before the drain began and are immutable now, so reading
                // them back eagerly is safe. Only pre-drain accumulation can spill (spillToMakeRoom
                // skips draining buffers), so this list is bounded by whatever arrived before the
                // consumer started — it does not grow during the stream.
                if (slot.spill != null) {
                    try {
                        this.spilled = slot.spill.readBack().iterator();
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to read back shuffle spill file " + slot.spill.path(), e);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                if (pending != null) {
                    return true;
                }
                if (spilled != null && spilled.hasNext()) {
                    pending = spilled.next();
                    return true;
                }
                if (eof) {
                    return false;
                }
                byte[] next;
                if (timeoutMillis <= 0) {
                    // Non-blocking mode (the legacy drain(slot) contract): yield only what is already
                    // resident and stop. An empty queue is a normal end here, NOT a stall.
                    next = slot.queue.poll();
                    if (next == null) {
                        eof = true;
                        return false;
                    }
                } else {
                    try {
                        next = slot.queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // Cancellation: surface it rather than reporting a clean end-of-stream, so the
                        // caller fails the native stream instead of silently under-delivering.
                        throw new IllegalStateException("Shuffle drain interrupted on slot " + slotLabel, e);
                    }
                }
                if (next == null) {
                    throw new IllegalStateException(
                        "Shuffle drain timed out after "
                            + timeoutMillis
                            + "ms waiting for the next chunk on slot "
                            + slotLabel
                            + " (received "
                            + slot.doneCount.get()
                            + "/"
                            + slot.expectedSenders
                            + " senders) — producers appear stalled"
                    );
                }
                if (next == EOF_SENTINEL) {
                    eof = true;
                    if (slot.invalidated) {
                        // End-of-stream reached, but the slot was invalidated (rows arrived after EOF —
                        // the declared sender count was too low). Finishing here would hand the join a
                        // truncated input and return a WRONG ANSWER with no error anywhere.
                        throw new IllegalStateException(
                            "Shuffle drain aborted on slot "
                                + slotLabel
                                + " — data arrived after end-of-stream, so this partition is truncated "
                                + "(declared "
                                + slot.expectedSenders
                                + " senders, "
                                + slot.doneCount.get()
                                + " reported done)"
                        );
                    }
                    return false;
                }
                if (next == CANCELLED_SENTINEL) {
                    eof = true;
                    throw new IllegalStateException(
                        "Shuffle drain aborted on slot " + slotLabel + " — the buffer was released (query terminated or cancelled)"
                    );
                }
                slot.queuedChunks.decrementAndGet();
                // Free the in-flight window as we consume, so blocked producers can proceed. This is
                // the feedback loop that paces producers to the consumer instead of accumulating.
                slot.queuedBytes.addAndGet(-next.length);
                pending = next;
                return true;
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Shuffle stream exhausted on slot " + slotLabel);
                }
                byte[] out = pending;
                pending = null;
                return out;
            }

            @Override
            public void close() {
                // The queue is owned by the buffer (cleared by removeBuffer/clearForQuery); the spill
                // file handle is closed by deleteSpillFiles on the terminal path. Nothing slot-local
                // to release here.
            }
        }

        /** Expected sender count declared for {@code slot}, or -1 if the slot was never declared. */
        public int getExpectedSenders(String slot) {
            Slot s = slots.get(slot);
            return s == null ? -1 : s.expectedSenders;
        }

        /** Number of senders that have reported {@code isLast} on {@code slot} (0 if unknown slot). */
        public int getDoneCount(String slot) {
            Slot s = slots.get(slot);
            return s == null ? 0 : s.doneCount.get();
        }

        /** Slot labels this buffer has seen (declared by the consumer or created by a producer). */
        public Set<String> getSlots() {
            return Set.copyOf(slots.keySet());
        }

        public int getExpectedLeftSenders() {
            return getExpectedSenders(ShuffleSlots.LEFT);
        }

        public int getExpectedRightSenders() {
            return getExpectedSenders(ShuffleSlots.RIGHT);
        }

        public int getLeftDoneCount() {
            return getDoneCount(ShuffleSlots.LEFT);
        }

        public int getRightDoneCount() {
            return getDoneCount(ShuffleSlots.RIGHT);
        }

        /**
         * Lazy drain iterator: yields spilled chunks streamed one-at-a-time from {@code spill}'s file
         * (deframing the big-endian length prefix), then the heap-resident {@code tail}. Only ONE
         * chunk is in heap at a time during the spilled phase — that is the property that lets an
         * over-budget partition drain through the consumer's bounded native channel without OOM.
         * Closing it releases the spill-file stream; it is also closed automatically at clean EOF of
         * the spilled phase. Reading past a corrupt/short frame surfaces an {@link UncheckedIOException}
         * (correctness over silent under-delivery — matches the eager {@code drainSide}).
         */
        private static final class SpillThenTailIterator implements CloseableIterator<byte[]> {
            private DataInputStream in;          // null once the spilled phase is exhausted/closed
            private final Iterator<byte[]> tail;
            private final Path spillPath;
            private byte[] nextChunk;            // one-chunk lookahead from the file
            private boolean nextLoaded;

            SpillThenTailIterator(SpilledSide spill, List<byte[]> tail) {
                this.tail = tail.iterator();
                this.spillPath = spill.path();
                try {
                    this.in = spill.openForRead();  // may be null if nothing was written
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to open shuffle spill file " + spillPath, e);
                }
            }

            @Override
            public boolean hasNext() {
                if (nextLoaded) {
                    return true;
                }
                if (in != null) {
                    nextChunk = readNextChunk();
                    if (nextChunk != null) {
                        nextLoaded = true;
                        return true;
                    }
                    // Spilled phase exhausted — release the file handle and fall through to the tail.
                    close();
                }
                return tail.hasNext();
            }

            @Override
            public byte[] next() {
                if (nextLoaded) {
                    byte[] c = nextChunk;
                    nextChunk = null;
                    nextLoaded = false;
                    return c;
                }
                if (in != null) {
                    byte[] c = readNextChunk();
                    if (c != null) {
                        return c;
                    }
                    close();
                }
                if (!tail.hasNext()) {
                    throw new NoSuchElementException();
                }
                return tail.next();
            }

            /** Reads one length-prefixed chunk, or {@code null} at clean EOF. */
            private byte[] readNextChunk() {
                try {
                    int len;
                    try {
                        len = in.readInt();
                    } catch (EOFException eof) {
                        return null; // clean end of file
                    }
                    byte[] chunk = new byte[len];
                    in.readFully(chunk);
                    return chunk;
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to read back shuffle spill file " + spillPath, e);
                }
            }

            @Override
            public void close() {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        LOGGER.debug(new ParameterizedMessage("Failed to close shuffle spill read stream {}", spillPath), e);
                    }
                    in = null;
                }
            }
        }

        /**
         * One side's on-disk spill file: a sequence of length-prefixed Arrow-IPC chunks
         * ({@code int length} big-endian, then {@code length} bytes), appended in arrival order.
         * Each chunk is already a self-describing Arrow IPC stream, so the frame just needs to record
         * its byte length to split the concatenation back apart on read.
         *
         * <p>Writes happen under the manager's {@code admitLock} (one writer at a time);
         * {@link #readBack} runs once at drain after the buffer is fully populated. The append stream
         * is buffered and flushed per chunk so a crash-free terminal can always read back the file.
         */
        private static final class SpilledSide {
            private final Path path;
            private OutputStream out;
            private long bytesOnDisk;

            SpilledSide(Path path) throws IOException {
                this.path = path;
                Files.createDirectories(path.getParent());
                // CREATE + truncate: a stale file at this exact (query,stage,partition,side) path can
                // only come from a prior incarnation that failed to clean up; start fresh.
                this.out = new BufferedOutputStream(Files.newOutputStream(path));
            }

            /** Appends one length-prefixed chunk and flushes so it is durably readable at drain. */
            void append(byte[] chunk) throws IOException {
                int len = chunk == null ? 0 : chunk.length;
                // Big-endian 4-byte length prefix, then the chunk bytes.
                out.write((len >>> 24) & 0xFF);
                out.write((len >>> 16) & 0xFF);
                out.write((len >>> 8) & 0xFF);
                out.write(len & 0xFF);
                if (len > 0) {
                    out.write(chunk);
                }
                out.flush();
                // Include the frame header so bytesOnDisk (released on cleanup) matches the framed size
                // reserved in spillOldest — else cleanup under-releases by 4×chunkCount. (codex round-2.)
                bytesOnDisk += len + SPILL_FRAME_HEADER_BYTES;
            }

            /**
             * Opens the spill file for sequential chunk-by-chunk reading. Closes the append stream
             * first so all buffered bytes are flushed. Returns {@code null} if no file was ever
             * written (the side spilled nothing). The caller owns the returned stream and must close
             * it. Used by the LAZY drain path so chunks are read one-at-a-time, never all-resident.
             */
            DataInputStream openForRead() throws IOException {
                closeOut();
                if (!Files.exists(path)) {
                    return null;
                }
                return new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
            }

            /**
             * Reads every spilled chunk back, in write (= arrival) order, deframing on the length
             * prefix. Closes the append stream first so all buffered bytes are flushed.
             */
            List<byte[]> readBack() throws IOException {
                closeOut();
                List<byte[]> chunks = new ArrayList<>();
                if (!Files.exists(path)) {
                    return chunks;
                }
                try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
                    while (true) {
                        int len;
                        try {
                            len = in.readInt();
                        } catch (EOFException eof) {
                            break; // clean end of file
                        }
                        byte[] chunk = new byte[len];
                        in.readFully(chunk);
                        chunks.add(chunk);
                    }
                }
                return chunks;
            }

            long bytesOnDisk() {
                return bytesOnDisk;
            }

            Path path() {
                return path;
            }

            private void closeOut() {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        LOGGER.debug(new ParameterizedMessage("Failed to close shuffle spill stream {}", path), e);
                    }
                    out = null;
                }
            }

            /**
             * Closes the append stream and deletes the spill file. Returns true if the file was deleted
             * or never existed (disk reclaimed); false if deletion failed and the file still occupies
             * disk — the caller then keeps the disk-budget bytes charged rather than over-releasing.
             */
            boolean closeAndDelete() {
                closeOut();
                try {
                    Files.deleteIfExists(path);
                    return true;
                } catch (IOException e) {
                    LOGGER.debug(new ParameterizedMessage("Failed to delete shuffle spill file {}", path), e);
                    return false;
                }
            }
        }
    }
}
