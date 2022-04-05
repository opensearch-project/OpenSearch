/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;

public class CopyState extends AbstractRefCounted {

    private final GatedCloseable<SegmentInfos> segmentInfosRef;
    private final ReplicationCheckpoint checkpoint;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final GatedCloseable<IndexCommit> commitRef;
    private final Set<StoreFileMetadata> pendingDeleteFiles;
    private final byte[] infosBytes;
    private final ShardId shardId;

    CopyState(IndexShard shard) throws IOException {
        super("replication-nrt-state");
        this.shardId = shard.shardId();
        this.segmentInfosRef = shard.getLatestSegmentInfosSafe();
        final SegmentInfos segmentInfos = segmentInfosRef.get();
        this.checkpoint = new ReplicationCheckpoint(
            shardId,
            shard.getOperationPrimaryTerm(),
            segmentInfos.getGeneration(),
            shard.getProcessedLocalCheckpoint(),
            segmentInfos.getVersion()
        );

        // Send files that are merged away in the latest SegmentInfos but not in the latest on disk Segments_N.
        // This ensures that the store on replicas is in sync with the store on primaries.
        final GatedCloseable<IndexCommit> lastIndexCommit = shard.acquireLastIndexCommit(false);
        final Store.MetadataSnapshot metadata = shard.store().getMetadata(lastIndexCommit.get());
        this.metadataSnapshot = shard.store().getMetadata(segmentInfos);
        final Store.RecoveryDiff diff = metadata.recoveryDiff(this.metadataSnapshot);
        this.pendingDeleteFiles = new HashSet<>(diff.missing);
        if (this.pendingDeleteFiles.isEmpty()) {
            // If there are no additional files we can release the last commit immediately.
            lastIndexCommit.close();
            this.commitRef = null;
        } else {
            this.commitRef = lastIndexCommit;
        }

        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput tmpIndexOutput = new ByteBuffersIndexOutput(buffer, "temporary", "temporary")) {
            segmentInfos.write(tmpIndexOutput);
        }
        this.infosBytes = buffer.toArrayCopy();
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public Store.MetadataSnapshot getMetadataSnapshot() {
        return metadataSnapshot;
    }

    public byte[] getInfosBytes() {
        return infosBytes;
    }

    public Set<StoreFileMetadata> getPendingDeleteFiles() {
        return pendingDeleteFiles;
    }

    @Override
    protected void closeInternal() {
        try {
            segmentInfosRef.close();
            if (commitRef != null) {
                commitRef.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        return "CopyState{"
            + "SegmentInfosRef="
            + segmentInfosRef
            + ", checkpoint="
            + checkpoint
            + ", metadataSnapshot="
            + metadataSnapshot
            + ", refcount="
            + refCount()
            + '}';
    }
}
