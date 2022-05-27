/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;

/**
 * An Opensearch-specific version of Lucene's CopyState class that
 * holds incRef'd file level details for one point-in-time segment infos.
 *
 * @opensearch.internal
 */
public class CopyState extends AbstractRefCounted {

    private final GatedCloseable<SegmentInfos> segmentInfosRef;
    private final ReplicationCheckpoint replicationCheckpoint;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final HashSet<StoreFileMetadata> pendingDeleteFiles;
    private final byte[] infosBytes;
    private GatedCloseable<IndexCommit> commitRef;

    public CopyState(IndexShard shard) throws IOException {
        super("CopyState-" + shard.shardId());
        this.segmentInfosRef = shard.getSegmentInfosSnapshot();
        SegmentInfos segmentInfos = this.segmentInfosRef.get();
        this.metadataSnapshot = shard.store().getMetadata(segmentInfos);
        this.replicationCheckpoint = new ReplicationCheckpoint(
            shard.shardId(),
            shard.getOperationPrimaryTerm(),
            segmentInfos.getGeneration(),
            shard.getProcessedLocalCheckpoint(),
            segmentInfos.getVersion()
        );

        // Send files that are merged away in the latest SegmentInfos but not in the latest on disk Segments_N.
        // This ensures that the store on replicas is in sync with the store on primaries.
        this.commitRef = shard.acquireLastIndexCommit(false);
        Store.MetadataSnapshot metadata = shard.store().getMetadata(this.commitRef.get());
        final Store.RecoveryDiff diff = metadata.recoveryDiff(this.metadataSnapshot);
        this.pendingDeleteFiles = new HashSet<>(diff.missing);
        if (this.pendingDeleteFiles.isEmpty()) {
            // If there are no additional files we can release the last commit immediately.
            this.commitRef.close();
            this.commitRef = null;
        }

        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        // resource description and name are not used, but resource description cannot be null
        try (ByteBuffersIndexOutput indexOutput = new ByteBuffersIndexOutput(buffer, "", null)) {
            segmentInfos.write(indexOutput);
        }
        this.infosBytes = buffer.toArrayCopy();
    }

    @Override
    protected void closeInternal() {
        try {
            segmentInfosRef.close();
            // commitRef may be null if there were no pending delete files
            if (commitRef != null) {
                commitRef.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ReplicationCheckpoint getCheckpoint() {
        return replicationCheckpoint;
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
}
