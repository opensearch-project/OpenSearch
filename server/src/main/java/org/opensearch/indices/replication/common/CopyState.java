/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * An Opensearch-specific version of Lucene's CopyState class that
 * holds incRef'd file level details for one point-in-time segment infos.
 *
 * @opensearch.internal
 */
public class CopyState extends AbstractRefCounted {

    private final GatedCloseable<SegmentInfos> segmentInfosRef;
    /** ReplicationCheckpoint requested */
    private final ReplicationCheckpoint requestedReplicationCheckpoint;
    /** Actual ReplicationCheckpoint returned by the shard */
    private final ReplicationCheckpoint replicationCheckpoint;
    private final Map<String, StoreFileMetadata> metadataMap;
    private final byte[] infosBytes;
    private final IndexShard shard;

    public CopyState(ReplicationCheckpoint requestedReplicationCheckpoint, IndexShard shard) throws IOException {
        super("CopyState-" + shard.shardId());
        this.requestedReplicationCheckpoint = requestedReplicationCheckpoint;
        this.shard = shard;
        final Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> latestSegmentInfosAndCheckpoint = shard
            .getLatestSegmentInfosAndCheckpoint();
        this.segmentInfosRef = latestSegmentInfosAndCheckpoint.v1();
        this.replicationCheckpoint = latestSegmentInfosAndCheckpoint.v2();
        SegmentInfos segmentInfos = this.segmentInfosRef.get();
        this.metadataMap = shard.store().getSegmentMetadataMap(segmentInfos);

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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ReplicationCheckpoint getCheckpoint() {
        return replicationCheckpoint;
    }

    public Map<String, StoreFileMetadata> getMetadataMap() {
        return metadataMap;
    }

    public byte[] getInfosBytes() {
        return infosBytes;
    }

    public IndexShard getShard() {
        return shard;
    }

    public ReplicationCheckpoint getRequestedReplicationCheckpoint() {
        return requestedReplicationCheckpoint;
    }
}
