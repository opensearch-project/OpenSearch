/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.store;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Metadata for store files
 *
 * @opensearch.internal
 */
public class StoreFilesMetadata implements Iterable<StoreFileMetadata>, Writeable {
    private final ShardId shardId;
    private final Store.MetadataSnapshot metadataSnapshot;
    private final List<RetentionLease> peerRecoveryRetentionLeases;

    public StoreFilesMetadata(ShardId shardId, Store.MetadataSnapshot metadataSnapshot, List<RetentionLease> peerRecoveryRetentionLeases) {
        this.shardId = shardId;
        this.metadataSnapshot = metadataSnapshot;
        this.peerRecoveryRetentionLeases = peerRecoveryRetentionLeases;
    }

    public StoreFilesMetadata(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.metadataSnapshot = new Store.MetadataSnapshot(in);
        this.peerRecoveryRetentionLeases = in.readList(RetentionLease::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        metadataSnapshot.writeTo(out);
        out.writeList(peerRecoveryRetentionLeases);
    }

    public ShardId shardId() {
        return this.shardId;
    }

    public boolean isEmpty() {
        return metadataSnapshot.size() == 0;
    }

    @Override
    public Iterator<StoreFileMetadata> iterator() {
        return metadataSnapshot.iterator();
    }

    public boolean fileExists(String name) {
        return metadataSnapshot.asMap().containsKey(name);
    }

    public StoreFileMetadata file(String name) {
        return metadataSnapshot.asMap().get(name);
    }

    /**
     * Returns the retaining sequence number of the peer recovery retention lease for a given node if exists; otherwise, returns -1.
     */
    public long getPeerRecoveryRetentionLeaseRetainingSeqNo(DiscoveryNode node) {
        assert node != null;
        final String retentionLeaseId = ReplicationTracker.getPeerRecoveryRetentionLeaseId(node.getId());
        return peerRecoveryRetentionLeases.stream()
            .filter(lease -> lease.id().equals(retentionLeaseId))
            .mapToLong(RetentionLease::retainingSequenceNumber)
            .findFirst()
            .orElse(-1L);
    }

    public List<RetentionLease> peerRecoveryRetentionLeases() {
        return peerRecoveryRetentionLeases;
    }

    /**
     * @return commit sync id if exists, else null
     */
    public String syncId() {
        return metadataSnapshot.getSyncId();
    }

    @Override
    public String toString() {
        return "StoreFilesMetadata{"
            + ", shardId="
            + shardId
            + ", metadataSnapshot{size="
            + metadataSnapshot.size()
            + ", syncId="
            + metadataSnapshot.getSyncId()
            + "}"
            + '}';
    }
}
