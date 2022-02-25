/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.io.UncheckedIOException;

public class CopyState extends AbstractRefCounted {

    private final Engine.SegmentInfosRef segmentInfosRef;
    private final ReplicationCheckpoint checkpoint;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final byte[] infosBytes;

    CopyState(IndexShard shard) throws IOException {
        super("replication-nrt-state");
        this.segmentInfosRef = shard.getLatestSegmentInfosSafe();
        final SegmentInfos segmentInfos = segmentInfosRef.getSegmentInfos();
        this.checkpoint = new ReplicationCheckpoint(
            shard.shardId(),
            shard.getOperationPrimaryTerm(),
            segmentInfos.getGeneration(),
            shard.getProcessedLocalCheckpoint()
        );
        this.metadataSnapshot = shard.store().getMetadata(segmentInfos);
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

    @Override
    protected void closeInternal() {
        try {
            segmentInfosRef.close();
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
