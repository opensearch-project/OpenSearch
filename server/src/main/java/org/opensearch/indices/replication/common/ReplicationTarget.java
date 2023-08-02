/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RateLimiter;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.recovery.RecoveryTransportRequest;
import org.opensearch.transport.TransportChannel;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the target of a replication operation performed on a shard
 *
 * @opensearch.internal
 */
public abstract class ReplicationTarget extends AbstractRefCounted {

    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    // last time the target/status was accessed
    private volatile long lastAccessTime = System.nanoTime();
    private final ReplicationRequestTracker requestTracker = new ReplicationRequestTracker();
    private final long id;

    protected final AtomicBoolean finished = new AtomicBoolean();
    protected final IndexShard indexShard;
    protected final Store store;
    protected final ReplicationListener listener;
    protected final Logger logger;
    protected final CancellableThreads cancellableThreads;
    protected final ReplicationLuceneIndex stateIndex;

    protected abstract String getPrefix();

    protected abstract void onDone();

    protected void onCancel(String reason) {
        cancellableThreads.cancel(reason);
    }

    public abstract ReplicationState state();

    public abstract ReplicationTarget retryCopy();

    public abstract String description();

    public ReplicationListener getListener() {
        return listener;
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    public abstract void notifyListener(ReplicationFailedException e, boolean sendShardFailure);

    public ReplicationTarget(String name, IndexShard indexShard, ReplicationLuceneIndex stateIndex, ReplicationListener listener) {
        super(name);
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.listener = listener;
        this.id = ID_GENERATOR.incrementAndGet();
        this.stateIndex = stateIndex;
        this.indexShard = indexShard;
        this.store = indexShard.store();
        // make sure the store is not released until we are done.
        this.cancellableThreads = new CancellableThreads();
        store.incRef();
    }

    public long getId() {
        return id;
    }

    public abstract boolean reset(CancellableThreads newTargetCancellableThreads) throws IOException;

    /**
     * return the last time this ReplicationStatus was used (based on System.nanoTime()
     */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /**
     * sets the lasAccessTime flag to now
     */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    @Nullable
    public ActionListener<Void> markRequestReceivedAndCreateListener(long requestSeqNo, ActionListener<Void> listener) {
        return requestTracker.markReceivedAndCreateListener(requestSeqNo, listener);
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public Store store() {
        ensureRefCount();
        return store;
    }

    public ShardId shardId() {
        return indexShard.shardId();
    }

    /**
     * mark the current replication as done
     */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            try {
                onDone();
            } finally {
                // release the initial reference. replication files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onDone(state());
        }
    }

    /**
     * cancel the replication. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("replication/recovery cancelled (reason: [{}])", reason);
                onCancel(reason);
            } finally {
                // release the initial reference. replication files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    /**
     * fail the replication and call listener
     *
     * @param e                exception that encapsulates the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(ReplicationFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("marking target " + description() + " as failed", e);
                notifyListener(e, sendShardFailure);
            } finally {
                try {
                    cancellableThreads.cancel("failed" + description() + "[" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    // release the initial reference. replication files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    protected void ensureRefCount() {
        if (refCount() <= 0) {
            throw new OpenSearchException(
                "ReplicationTarget is used but it's refcount is 0. Probably a mismatch between incRef/decRef calls"
            );
        }
    }

    @Nullable
    public ActionListener<Void> createOrFinishListener(
        final TransportChannel channel,
        final String action,
        final RecoveryTransportRequest request
    ) {
        return createOrFinishListener(channel, action, request, nullVal -> TransportResponse.Empty.INSTANCE);
    }

    @Nullable
    public ActionListener<Void> createOrFinishListener(
        final TransportChannel channel,
        final String action,
        final RecoveryTransportRequest request,
        final CheckedFunction<Void, TransportResponse, Exception> responseFn
    ) {
        final ActionListener<TransportResponse> channelListener = new ChannelActionListener<>(channel, action, request);
        final ActionListener<Void> voidListener = ActionListener.map(channelListener, responseFn);

        final long requestSeqNo = request.requestSeqNo();
        final ActionListener<Void> listener;
        if (requestSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            listener = markRequestReceivedAndCreateListener(requestSeqNo, voidListener);
        } else {
            listener = voidListener;
        }

        return listener;
    }

    /**
     * Handle a FileChunkRequest for a {@link ReplicationTarget}.
     *
     * @param request {@link FileChunkRequest} Request containing the file chunk.
     * @param bytesSinceLastPause {@link AtomicLong} Bytes since the last pause.
     * @param rateLimiter {@link RateLimiter} Rate limiter.
     * @param listener {@link ActionListener} listener that completes when the chunk has been written.
     * @throws IOException When there is an issue pausing the rate limiter.
     */
    public void handleFileChunk(
        final FileChunkRequest request,
        final ReplicationTarget replicationTarget,
        final AtomicLong bytesSinceLastPause,
        final RateLimiter rateLimiter,
        final ActionListener<Void> listener
    ) throws IOException {

        if (listener == null) {
            return;
        }
        final ReplicationLuceneIndex indexState = replicationTarget.state().getIndex();
        if (request.sourceThrottleTimeInNanos() != ReplicationLuceneIndex.UNKNOWN) {
            indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
        }
        if (rateLimiter != null) {
            long bytes = bytesSinceLastPause.addAndGet(request.content().length());
            if (bytes > rateLimiter.getMinPauseCheckBytes()) {
                // Time to pause
                bytesSinceLastPause.addAndGet(-bytes);
                long throttleTimeInNanos = rateLimiter.pause(bytes);
                indexState.addTargetThrottling(throttleTimeInNanos);
                replicationTarget.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
            }
        }
        writeFileChunk(
            request.metadata(),
            request.position(),
            request.content(),
            request.lastChunk(),
            request.totalTranslogOps(),
            listener
        );
    }

    public abstract void writeFileChunk(
        StoreFileMetadata metadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    );

    protected void closeInternal() {
        store.decRef();
    }
}
