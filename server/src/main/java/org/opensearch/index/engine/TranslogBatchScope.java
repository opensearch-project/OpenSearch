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
    private final BiConsumer<String, Exception> failEngine;
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
        BiConsumer<String, Exception> failEngine,
        Consumer<TranslogBatchScope> onFinished
    ) {
        this(translogManager, localCheckpointTracker, shardId, failEngine, onFinished, DEFAULT_MAX_OPERATIONS, DEFAULT_MAX_BYTES);
    }

    TranslogBatchScope(
        TranslogManager translogManager,
        LocalCheckpointTracker localCheckpointTracker,
        ShardId shardId,
        BiConsumer<String, Exception> failEngine,
        Consumer<TranslogBatchScope> onFinished,
        int maxOperations,
        long maxBytes
    ) {
        assert maxOperations >= 1 : maxOperations;
        assert maxBytes >= 1 : maxBytes;
        this.translogManager = translogManager;
        this.localCheckpointTracker = localCheckpointTracker;
        this.shardId = shardId;
        this.failEngine = failEngine;
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
            final EngineException appendFailure = (ex instanceof EngineException)
                ? (EngineException) ex
                : new EngineException(shardId, "failed to append batched translog chunk of [" + chunk.size() + "] operations", ex);
            failure = appendFailure;
            entries.clear();
            pendingBytes = 0L;
            for (Entry entry : chunk) {
                if (entry.pending != null) {
                    entry.pending.completeExceptionally(appendFailure);
                }
            }
            onFinished.accept(this);
            failEngine.accept("failed to append batched translog chunk", ex);
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
