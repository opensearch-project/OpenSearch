/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;

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
    private final ShardId shardId;
    protected final IndexShard indexShard;
    protected final ReplicationListener listener;
    protected final Logger logger;
    protected final CancellableThreads cancellableThreads;
    protected final ReplicationLuceneIndex stateIndex;

    protected abstract String getPrefix();

    protected abstract void onDone();

    protected abstract void onCancel(String reason);

    public abstract ReplicationState state();

    public abstract ReplicationTarget retryCopy();

    public abstract String description();

    public ReplicationListener getListener() {
        return listener;
    }

    public CancellableThreads cancellableThreads() {
        return cancellableThreads;
    }

    public abstract void notifyListener(Exception e, boolean sendShardFailure);

    public ReplicationTarget(String name, IndexShard indexShard, ReplicationLuceneIndex stateIndex, ReplicationListener listener) {
        super(name);
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.listener = listener;
        this.id = ID_GENERATOR.incrementAndGet();
        this.stateIndex = stateIndex;
        this.indexShard = indexShard;
        this.shardId = indexShard.shardId();
        // make sure the store is not released until we are done.
        this.cancellableThreads = new CancellableThreads();
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

    public ActionListener<Void> markRequestReceivedAndCreateListener(long requestSeqNo, ActionListener<Void> listener) {
        return requestTracker.markReceivedAndCreateListener(requestSeqNo, listener);
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public ShardId shardId() {
        return shardId;
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
                logger.debug("replication cancelled (reason: [{}])", reason);
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
    public void fail(OpenSearchException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
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

}
