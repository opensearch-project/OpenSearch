/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class has the common code used in {@link TransportNodesListShardStoreMetadata} and
 * {@link TransportNodesListShardStoreMetadataBatch} to get the shard info on the local node.
 * <p>
 * This class should not be used to add more functions and will be removed when the
 * {@link TransportNodesListShardStoreMetadata} will be deprecated and all the code will be moved to
 * {@link TransportNodesListShardStoreMetadataBatch}
 *
 * @opensearch.internal
 */
public class TransportNodesListShardStoreMetadataHelper {

    public static final String INDEX_NOT_FOUND = "node doesn't have meta data for index ";

    public static StoreFilesMetadata listShardMetadataInternal(
        Logger logger,
        final ShardId shardId,
        NodeEnvironment nodeEnv,
        IndicesService indicesService,
        String customDataPath,
        Settings settings,
        ClusterService clusterService
    ) throws IOException {
        logger.trace("listing store meta data for {}", shardId);
        long startTimeNS = System.nanoTime();
        boolean exists = false;
        try {
            IndexService indexService = indicesService.indexService(shardId.getIndex());
            if (indexService != null) {
                IndexShard indexShard = indexService.getShardOrNull(shardId.id());
                if (indexShard != null) {
                    try {
                        final StoreFilesMetadata storeFilesMetadata = new StoreFilesMetadata(
                            shardId,
                            indexShard.snapshotStoreMetadata(),
                            indexShard.getPeerRecoveryRetentionLeases()
                        );
                        exists = true;
                        return storeFilesMetadata;
                    } catch (org.apache.lucene.index.IndexNotFoundException e) {
                        logger.trace(new ParameterizedMessage("[{}] node is missing index, responding with empty", shardId), e);
                        return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
                    } catch (IOException e) {
                        logger.warn(new ParameterizedMessage("[{}] can't read metadata from store, responding with empty", shardId), e);
                        return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
                    }
                }
            }
            if (customDataPath == null) {
                // TODO: Fallback for BWC with older predecessor (ES) versions.
                // Remove this once request.getCustomDataPath() always returns non-null
                if (indexService != null) {
                    customDataPath = indexService.getIndexSettings().customDataPath();
                } else {
                    IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
                    if (metadata != null) {
                        customDataPath = new IndexSettings(metadata, settings).customDataPath();
                    } else {
                        logger.trace("{} node doesn't have meta data for the requests index", shardId);
                        throw new OpenSearchException(INDEX_NOT_FOUND + shardId.getIndex());
                    }
                }
            }
            final ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
            if (shardPath == null) {
                return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
            }
            // note that this may fail if it can't get access to the shard lock. Since we check above there is an active shard, this means:
            // 1) a shard is being constructed, which means the cluster-manager will not use a copy of this replica
            // 2) A shard is shutting down and has not cleared it's content within lock timeout. In this case the cluster-manager may not
            // reuse local resources.
            final Store.MetadataSnapshot metadataSnapshot = Store.readMetadataSnapshot(
                shardPath.resolveIndex(),
                shardId,
                nodeEnv::shardLock,
                logger
            );
            // We use peer recovery retention leases from the primary for allocating replicas. We should always have retention leases when
            // we refresh shard info after the primary has started. Hence, we can ignore retention leases if there is no active shard.
            return new StoreFilesMetadata(shardId, metadataSnapshot, Collections.emptyList());
        } finally {
            TimeValue took = new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS);
            if (exists) {
                logger.debug("{} loaded store meta data (took [{}])", shardId, took);
            } else {
                logger.trace("{} didn't find any store meta data to load (took [{}])", shardId, took);
            }
        }
    }

    /**
     * Metadata for store files
     *
     * @opensearch.internal
     */
    public static class StoreFilesMetadata implements Iterable<StoreFileMetadata>, Writeable {
        private final ShardId shardId;
        private final Store.MetadataSnapshot metadataSnapshot;
        private final List<RetentionLease> peerRecoveryRetentionLeases;

        public StoreFilesMetadata(
            ShardId shardId,
            Store.MetadataSnapshot metadataSnapshot,
            List<RetentionLease> peerRecoveryRetentionLeases
        ) {
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
}
