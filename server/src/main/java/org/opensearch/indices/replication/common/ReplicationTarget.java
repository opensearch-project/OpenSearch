/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.MultiFileWriter;
import org.opensearch.indices.recovery.RecoveryIndex;
import org.opensearch.indices.recovery.RecoveryRequestTracker;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ReplicationTarget extends AbstractRefCounted {

    // TODO will this cause issues because its shared between subclasses?
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    // last time the target/status was accessed
    private volatile long lastAccessTime = System.nanoTime();
    private final RecoveryRequestTracker requestTracker = new RecoveryRequestTracker();
    private final long id;

    protected final AtomicBoolean finished = new AtomicBoolean();
    protected final IndexShard indexShard;
    protected final Store store;
    protected final ReplicationListener listener;
    protected final MultiFileWriter multiFileWriter;
    protected final Logger logger;
    protected final RecoveryIndex recoveryStateIndex;

    protected abstract String getPrefix();

    protected abstract void onDone();

    protected abstract void onCancel(String reason);

    protected abstract void onFail(OpenSearchException e, boolean sendShardFailure);

    public abstract ReplicationState state();

    public ReplicationTarget(String name, IndexShard indexShard, RecoveryIndex recoveryStateIndex, ReplicationListener listener) {
        super(name);
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.listener = listener;
        this.id = ID_GENERATOR.incrementAndGet();
        this.recoveryStateIndex = recoveryStateIndex;
        this.indexShard = indexShard;
        this.store = indexShard.store();
        final String tempFilePrefix = getPrefix() + UUIDs.randomBase64UUID() + ".";
        this.multiFileWriter = new MultiFileWriter(indexShard.store(), recoveryStateIndex, tempFilePrefix, logger, this::ensureRefCount);
        // make sure the store is not released until we are done.
        store.incRef();
    }

    public long getId() {
        return id;
    }

    /**
     * return the last time this RecoveryStatus was used (based on System.nanoTime()
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

    public Store store() {
        ensureRefCount();
        return store;
    }

    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        ActionListener<Void> actionListener
    ) {
        try {
            multiFileWriter.writeFileChunk(fileMetadata, position, content, lastChunk);
            actionListener.onResponse(null);
        } catch (Exception e) {
            actionListener.onFailure(e);
        }
    }

    /**
     * mark the current recovery as done
     */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            try {
                onDone();
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onDone(state());
        }
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                onCancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(OpenSearchException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                listener.onFailure(state(), e, sendShardFailure);
            } finally {
                try {
                    onFail(e, sendShardFailure);
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    protected void ensureRefCount() {
        if (refCount() <= 0) {
            throw new OpenSearchException("RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef calls");
        }
    }

    @Override
    protected void closeInternal() {
        try {
            multiFileWriter.close();
        } finally {
            // free store. increment happens in constructor
            store.decRef();
        }
    }

    public abstract DiscoveryNode sourceNode();
}
