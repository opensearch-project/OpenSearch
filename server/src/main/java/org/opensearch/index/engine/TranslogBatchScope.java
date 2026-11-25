/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.Nullable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A reusable, bounded translog batch scope shared by remote-store segment-replication engines.
 *
 * @opensearch.internal
 */
final class TranslogBatchScope implements Engine.TranslogBatch, IndexVersionValue.PendingLocation.Flusher {

    static final int DEFAULT_MAX_OPERATIONS = 1_000;
    static final long DEFAULT_MAX_BYTES = 1L << 20;

    private static final class Entry {
        private final Translog.Operation operation;
        private final Engine.IndexResult result;
        @Nullable
        private final IndexVersionValue.PendingLocation pending;
        private final long seqNo;

        private Entry(
            Translog.Operation operation,
            Engine.IndexResult result,
            @Nullable IndexVersionValue.PendingLocation pending,
            long seqNo
        ) {
            this.operation = operation;
            this.result = result;
            this.pending = pending;
            this.seqNo = seqNo;
        }
    }

    private final TranslogManager translogManager;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final ShardId shardId;
    private final BiConsumer<String, Exception> maybeFailEngine;
    private final Consumer<TranslogBatchScope> onFinished;
    private final int maxOperations;
    private final long maxBytes;
    private final List<Entry> entries = new ArrayList<>();

    private long pendingBytes;
    private Translog.Location maxLocation;
    private RuntimeException failure;
    private boolean finished;

    TranslogBatchScope(
        TranslogManager translogManager,
        LocalCheckpointTracker localCheckpointTracker,
        ShardId shardId,
        BiConsumer<String, Exception> maybeFailEngine,
        Consumer<TranslogBatchScope> onFinished
    ) {
        this(translogManager, localCheckpointTracker, shardId, maybeFailEngine, onFinished, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_BYTES);
    }

    TranslogBatchScope(
        TranslogManager translogManager,
        LocalCheckpointTracker localCheckpointTracker,
        ShardId shardId,
        BiConsumer<String, Exception> maybeFailEngine,
        Consumer<TranslogBatchScope> onFinished,
        int maxOperations,
        long maxBytes
    ) {
        assert maxOperations >= 1 : maxOperations;
        assert maxBytes >= 1 : maxBytes;
        this.translogManager = translogManager;
        this.localCheckpointTracker = localCheckpointTracker;
        this.shardId = shardId;
        this.maybeFailEngine = maybeFailEngine;
        this.onFinished = onFinished;
        this.maxOperations = maxOperations;
        this.maxBytes = maxBytes;
    }

    synchronized void add(
        Translog.Operation operation,
        Engine.IndexResult result,
        @Nullable IndexVersionValue.PendingLocation pending,
        long seqNo
    ) {
        ensureActive();
        final long operationBytes = Math.max(1L, operation.estimateSize());
        if (entries.isEmpty() == false && (entries.size() >= maxOperations || pendingBytes + operationBytes > maxBytes)) {
            flushChunk();
        }
        entries.add(new Entry(operation, result, pending, seqNo));
        pendingBytes += operationBytes;
        if (entries.size() >= maxOperations || pendingBytes >= maxBytes) {
            flushChunk();
        }
    }

    @Override
    public void flushForPendingRead() {
        flush();
    }

    @Override
    public synchronized Translog.Location flush() {
        if (finished) {
            if (failure != null) {
                throw failure;
            }
            return maxLocation;
        }
        ensureActive();
        flushChunk();
        return maxLocation;
    }

    @Override
    public synchronized Translog.Location finish() {
        if (finished) {
            if (failure != null) {
                throw failure;
            }
            return maxLocation;
        }
        try {
            ensureActive();
            flushChunk();
            return maxLocation;
        } finally {
            finished = true;
            onFinished.accept(this);
        }
    }

    synchronized void abort(RuntimeException closeFailure) {
        if (finished) {
            return;
        }
        failure = closeFailure;
        finished = true;
        for (Entry entry : entries) {
            if (entry.pending != null) {
                entry.pending.completeExceptionally(closeFailure);
            }
        }
        entries.clear();
        pendingBytes = 0L;
        onFinished.accept(this);
    }

    private void ensureActive() {
        if (failure != null) {
            throw failure;
        }
        if (finished) {
            throw new IllegalStateException("translog batch scope is already finished");
        }
    }

    private void flushChunk() {
        if (entries.isEmpty()) {
            return;
        }
        final List<Entry> chunk = new ArrayList<>(entries);
        final List<Translog.Operation> operations = new ArrayList<>(chunk.size());
        for (Entry entry : chunk) {
            operations.add(entry.operation);
        }

        final Translog.Location[] locations;
        try {
            locations = translogManager.add(operations);
        } catch (Exception ex) {
            // Identical handling to a per-operation Translog#add failing inside InternalEngine#index: the translog has
            // already recorded its own tragic event (closeOnTragicEvent) if the failure was one, the engine is asked
            // maybeFailEngine with the raw exception so it fails only when that exception IS the tragic event (or an
            // AlreadyClosedException over one), and the request fails with the same exception the single-operation path
            // would have thrown. In particular an AlreadyClosedException is surfaced as itself so that
            // TransportActions#isShardNotAvailableException still holds and the coordinating node retries on the
            // re-promoted primary. Only the checked IOException needs an unchecked carrier for this interface; the
            // translog's own TranslogException is used, as Translog#add does for its non-IO failures.
            final RuntimeException appendFailure = (ex instanceof RuntimeException)
                ? (RuntimeException) ex
                : new TranslogException(shardId, "Failed to write batch of [" + chunk.size() + "] operations", ex);
            failure = appendFailure;
            entries.clear();
            pendingBytes = 0L;
            for (Entry entry : chunk) {
                if (entry.pending != null) {
                    entry.pending.completeExceptionally(appendFailure);
                }
            }
            onFinished.accept(this);
            maybeFailEngine.accept("translog batch append", ex);
            throw appendFailure;
        }

        assert locations.length == chunk.size();
        entries.clear();
        pendingBytes = 0L;
        for (int i = 0; i < chunk.size(); i++) {
            final Entry entry = chunk.get(i);
            final Translog.Location location = locations[i];
            entry.result.setTranslogLocation(location);
            entry.result.freeze();
            localCheckpointTracker.markSeqNoAsProcessed(entry.seqNo);
            if (entry.pending != null) {
                entry.pending.complete(location);
            }
            if (maxLocation == null || location.compareTo(maxLocation) > 0) {
                maxLocation = location;
            }
        }
    }
}
