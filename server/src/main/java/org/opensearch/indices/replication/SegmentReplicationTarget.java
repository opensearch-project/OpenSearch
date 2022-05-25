/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationTarget;

import java.io.IOException;

/**
 * Represents the target of a replication event.
 *
 * @opensearch.internal
 */
public class SegmentReplicationTarget extends ReplicationTarget {

    private final ReplicationCheckpoint checkpoint;
    private final SegmentReplicationSource source;
    private final SegmentReplicationState state;

    public SegmentReplicationTarget(
        ReplicationCheckpoint checkpoint,
        IndexShard indexShard,
        SegmentReplicationSource source,
        SegmentReplicationTargetService.SegmentReplicationListener listener
    ) {
        super("replication_target", indexShard, new ReplicationLuceneIndex(), listener);
        this.checkpoint = checkpoint;
        this.source = source;
        this.state = new SegmentReplicationState();
    }

    @Override
    protected void closeInternal() {
        // TODO
    }

    @Override
    protected String getPrefix() {
        // TODO
        return null;
    }

    @Override
    protected void onDone() {
        this.state.setStage(SegmentReplicationState.Stage.DONE);
    }

    @Override
    protected void onCancel(String reason) {
        // TODO
    }

    @Override
    public ReplicationState state() {
        return state;
    }

    @Override
    public ReplicationTarget retryCopy() {
        // TODO
        return null;
    }

    @Override
    public String description() {
        // TODO
        return null;
    }

    @Override
    public void notifyListener(OpenSearchException e, boolean sendShardFailure) {
        listener.onFailure(state(), e, sendShardFailure);
    }

    @Override
    public boolean reset(CancellableThreads newTargetCancellableThreads) throws IOException {
        // TODO
        return false;
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata metadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        // TODO
    }

    /**
     * Start the Replication event.
     * @param listener {@link ActionListener} listener.
     */
    public void startReplication(ActionListener<Void> listener) {
        // TODO
    }
}
