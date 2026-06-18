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
import org.opensearch.analytics.spi.ShuffleBufferAccess;
import org.opensearch.analytics.spi.ShuffleBufferExceededException;
import org.opensearch.analytics.spi.ShuffleBufferRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    private final AtomicLong totalBytes = new AtomicLong();
    private final Map<String, AtomicLong> perQueryBytes = new ConcurrentHashMap<>();
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
            ShuffleBuffer buffer = buffers.computeIfAbsent(key(queryId, targetStageId, partitionIndex), k -> new ShuffleBuffer());
            AtomicLong q = perQueryBytes.computeIfAbsent(queryId, k -> new AtomicLong());
            long qProjected = q.get() + size;
            if (qProjected > perQueryMaxBytes) {
                buffer.recordRejected();
                // Hard: this query alone can't fit — waiting never helps. Fail fast, non-retryable.
                throw new ShuffleBufferExceededException(qProjected, perQueryMaxBytes);
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

    public ShuffleBuffer getOrCreateBuffer(String queryId, int targetStageId, int partitionIndex) {
        // If the query was already cleared (cancelled/terminated), do NOT repopulate the map — a late
        // producer RPC racing after clearForQuery would otherwise leave an undrained buffer behind.
        // Hand back an unstored throwaway so the write lands on a collectible object. (This path is
        // the isLast/senderDone case — it creates a 0-byte buffer and reserves no budget.)
        if (isTombstoned(queryId)) {
            return new ShuffleBuffer();
        }
        String k = key(queryId, targetStageId, partitionIndex);
        ShuffleBuffer buffer = buffers.computeIfAbsent(k, kk -> new ShuffleBuffer());
        // Double-check AFTER inserting: the tombstone-then-check above is not atomic with the
        // computeIfAbsent, so a producer that read not-tombstoned can still insert a key AFTER
        // clearForQuery's removal sweep already passed it. clearForQuery sets the tombstone BEFORE
        // sweeping, so any such late insert is guaranteed to observe the tombstone here — remove our
        // own entry and return a throwaway, closing the recreate race. (codex review: tombstone race.)
        if (isTombstoned(queryId)) {
            buffers.remove(k, buffer);
            return new ShuffleBuffer();
        }
        return buffer;
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
        synchronized (admitLock) {
            ShuffleBuffer removed = buffers.remove(key(queryId, targetStageId, partitionIndex));
            if (removed != null) {
                releaseLocked(queryId, removed.getCurrentBytes());
            }
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
                if (it.next().getKey().startsWith(prefix)) {
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
        if (removed > 0) {
            LOGGER.debug("Cleared {} shuffle buffer(s) for query {} (freed budget)", removed, queryId);
        }
        return removed;
    }

    private static String key(String queryId, int targetStageId, int partitionIndex) {
        return queryId + ":" + targetStageId + ":" + partitionIndex;
    }

    /**
     * Per-partition shuffle buffer — a store + completion latches. Thread-safe: multiple senders
     * call {@link #addData} concurrently (after the manager's {@link #tryAdmit} reserves budget);
     * one consumer thread drains via {@link #getLeftData} / {@link #getRightData} after
     * {@link #awaitReady} returns. Byte-budget enforcement lives in the enclosing manager, not here.
     */
    public static class ShuffleBuffer implements ShuffleBufferAccess {
        private final List<byte[]> leftData = Collections.synchronizedList(new ArrayList<>());
        private final List<byte[]> rightData = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger leftDoneCount = new AtomicInteger();
        private final AtomicInteger rightDoneCount = new AtomicInteger();
        private volatile int expectedLeftSenders = -1;
        private volatile int expectedRightSenders = -1;
        private final CountDownLatch leftReady = new CountDownLatch(1);
        private final CountDownLatch rightReady = new CountDownLatch(1);
        private final AtomicLong currentBytes = new AtomicLong();
        private final AtomicLong rejectedCount = new AtomicLong();

        public ShuffleBuffer() {}

        public void setExpectedSenders(int leftSenders, int rightSenders) {
            // -1 means "leave unchanged"; allows per-side handlers to set their own count
            // without clobbering the other side's.
            if (leftSenders >= 0) {
                this.expectedLeftSenders = leftSenders;
                checkCompletion("left");
            }
            if (rightSenders >= 0) {
                this.expectedRightSenders = rightSenders;
                checkCompletion("right");
            }
        }

        /**
         * Stores {@code data} on the named side and tracks this buffer's byte total. Admission /
         * budget enforcement is owned by the enclosing {@link ShuffleBufferManager#tryAdmit} (node +
         * per-query budgets); this method only stores after admission has reserved the bytes. The
         * tracked {@link #currentBytes} is read at removal time to release the reservation.
         */
        public void addData(String side, byte[] data) {
            int size = data == null ? 0 : data.length;
            currentBytes.addAndGet(size);
            if ("left".equals(side)) {
                leftData.add(data);
            } else {
                rightData.add(data);
            }
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

        public void senderDone(String side) {
            if ("left".equals(side)) {
                leftDoneCount.incrementAndGet();
                checkCompletion("left");
            } else {
                rightDoneCount.incrementAndGet();
                checkCompletion("right");
            }
        }

        private void checkCompletion(String side) {
            if ("left".equals(side)) {
                if (expectedLeftSenders >= 0 && leftDoneCount.get() >= expectedLeftSenders) {
                    leftReady.countDown();
                }
            } else {
                if (expectedRightSenders >= 0 && rightDoneCount.get() >= expectedRightSenders) {
                    rightReady.countDown();
                }
            }
        }

        /** Wait for both sides' senders to complete. Returns {@code false} on timeout. */
        public boolean awaitReady(long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            long remaining = timeoutMillis;
            if (!leftReady.await(remaining, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Shuffle left side timed out: received {}/{} senders", leftDoneCount.get(), expectedLeftSenders);
                return false;
            }
            remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) return false;
            if (!rightReady.await(remaining, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Shuffle right side timed out: received {}/{} senders", rightDoneCount.get(), expectedRightSenders);
                return false;
            }
            return true;
        }

        public List<byte[]> getLeftData() {
            return leftData;
        }

        public List<byte[]> getRightData() {
            return rightData;
        }

        public int getExpectedLeftSenders() {
            return expectedLeftSenders;
        }

        public int getExpectedRightSenders() {
            return expectedRightSenders;
        }

        public int getLeftDoneCount() {
            return leftDoneCount.get();
        }

        public int getRightDoneCount() {
            return rightDoneCount.get();
        }
    }
}
