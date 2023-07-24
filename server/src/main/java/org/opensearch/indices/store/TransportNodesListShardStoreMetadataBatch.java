/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.store;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.AsyncBatchShardFetch;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Transport action for fetching the batch of shard stores Metadata from a list of transport nodes
 *
 * @opensearch.internal
 */
public class TransportNodesListShardStoreMetadataBatch extends TransportNodesAction<
    TransportNodesListShardStoreMetadataBatch.Request,
    TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch,
    TransportNodesListShardStoreMetadataBatch.NodeRequest,
    TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch>
    implements
        AsyncBatchShardFetch.Lister<
            TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch,
            TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadataBatch> {

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/shard/store/batch";
    public static final ActionType<TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch> TYPE = new ActionType<>(
        ACTION_NAME,
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch::new
    );

    private final Settings settings;
    private final IndicesService indicesService;
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportNodesListShardStoreMetadataBatch(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        NodeEnvironment nodeEnv,
        ActionFilters actionFilters
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STORE,
            NodeStoreFilesMetadataBatch.class
        );
        this.settings = settings;
        this.indicesService = indicesService;
        this.nodeEnv = nodeEnv;
    }

    @Override
    public void list(
        DiscoveryNode[] nodes,
        Map<ShardId, String> shardIdsWithCustomDataPath,
        ActionListener<NodesStoreFilesMetadataBatch> listener
    ) {
        execute(new TransportNodesListShardStoreMetadataBatch.Request(shardIdsWithCustomDataPath, nodes), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeStoreFilesMetadataBatch newNodeResponse(StreamInput in) throws IOException {
        return new NodeStoreFilesMetadataBatch(in);
    }

    @Override
    protected NodesStoreFilesMetadataBatch newResponse(
        Request request,
        List<NodeStoreFilesMetadataBatch> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesStoreFilesMetadataBatch(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStoreFilesMetadataBatch nodeOperation(NodeRequest request) {
        try {
            return new NodeStoreFilesMetadataBatch(clusterService.localNode(), listStoreMetadata(request));
        } catch (IOException e) {
            throw new OpenSearchException("Failed to list store metadata for shards [" + request.getShardIdsWithCustomDataPath() + "]", e);
        }
    }

    /**
     * This method is similar to listStoreMetadata method of {@link TransportNodesListShardStoreMetadata}
     * In this case we fetch the shard store files for batch of shards instead of one shard.
     */
    private Map<ShardId, NodeStoreFilesMetadata> listStoreMetadata(NodeRequest request) throws IOException {
        Map<ShardId, NodeStoreFilesMetadata> shardStoreMetadataMap = new HashMap<ShardId, NodeStoreFilesMetadata>();
        for (Map.Entry<ShardId, String> shardToCustomDataPathEntry : request.getShardIdsWithCustomDataPath().entrySet()) {
            final ShardId shardId = shardToCustomDataPathEntry.getKey();
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
                            shardStoreMetadataMap.put(shardId, new NodeStoreFilesMetadata(storeFilesMetadata, null));
                            continue;
                        } catch (org.apache.lucene.index.IndexNotFoundException e) {
                            logger.trace(new ParameterizedMessage("[{}] node is missing index, responding with empty", shardId), e);
                            shardStoreMetadataMap.put(
                                shardId,
                                new NodeStoreFilesMetadata(
                                    new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList()),
                                    e
                                )
                            );
                            continue;
                        } catch (IOException e) {
                            logger.warn(new ParameterizedMessage("[{}] can't read metadata from store, responding with empty", shardId), e);
                            shardStoreMetadataMap.put(
                                shardId,
                                new NodeStoreFilesMetadata(
                                    new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList()),
                                    e
                                )
                            );
                            continue;
                        }
                    }
                }
                final String customDataPath;
                if (shardToCustomDataPathEntry.getValue() != null) {
                    customDataPath = shardToCustomDataPathEntry.getValue();
                } else {
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
                            shardStoreMetadataMap.put(
                                shardId,
                                new NodeStoreFilesMetadata(
                                    new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList()),
                                    new OpenSearchException("node doesn't have meta data for index " + shardId.getIndex())
                                )
                            );
                            continue;
                        }
                    }
                }
                final ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
                if (shardPath == null) {
                    shardStoreMetadataMap.put(
                        shardId,
                        new NodeStoreFilesMetadata(
                            new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList()),
                            null
                        )
                    );
                    continue;
                }
                // note that this may fail if it can't get access to the shard lock. Since we check above there is an active shard, this
                // means:
                // 1) a shard is being constructed, which means the cluster-manager will not use a copy of this replica
                // 2) A shard is shutting down and has not cleared it's content within lock timeout. In this case the cluster-manager may
                // not
                // reuse local resources.
                final Store.MetadataSnapshot metadataSnapshot = Store.readMetadataSnapshot(
                    shardPath.resolveIndex(),
                    shardId,
                    nodeEnv::shardLock,
                    logger
                );
                // We use peer recovery retention leases from the primary for allocating replicas. We should always have retention leases
                // when
                // we refresh shard info after the primary has started. Hence, we can ignore retention leases if there is no active shard.
                shardStoreMetadataMap.put(
                    shardId,
                    new NodeStoreFilesMetadata(new StoreFilesMetadata(shardId, metadataSnapshot, Collections.emptyList()), null)
                );
            } catch (Exception e) {
                logger.trace("{} failed to load store metadata {}", shardId, e);
                shardStoreMetadataMap.put(
                    shardId,
                    new NodeStoreFilesMetadata(
                        new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList()),
                        new OpenSearchException("failed to load store metadata", e)
                    )
                );
            } finally {
                TimeValue took = new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS);
                if (exists) {
                    logger.debug("{} loaded store meta data (took [{}])", shardId, took);
                } else {
                    logger.trace("{} didn't find any store meta data to load (took [{}])", shardId, took);
                }
            }
        }
        return shardStoreMetadataMap;
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

    /**
     * Request is used in constructing the request for making the transport request to set of other node.
     * Refer {@link TransportNodesAction} class start method.
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {

        private final Map<ShardId, String> shardIdsWithCustomDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardIdsWithCustomDataPath = in.readMap(ShardId::new, StreamInput::readString);
        }

        public Request(Map<ShardId, String> shardIdsWithCustomDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardIdsWithCustomDataPath = Objects.requireNonNull(shardIdsWithCustomDataPath);
        }

        public Map<ShardId, String> getShardIdsWithCustomDataPath() {
            return shardIdsWithCustomDataPath;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdsWithCustomDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }
    }

    /**
     * Metadata for the nodes store files
     *
     * @opensearch.internal
     */
    public static class NodesStoreFilesMetadataBatch extends BaseNodesResponse<NodeStoreFilesMetadataBatch> {

        public NodesStoreFilesMetadataBatch(StreamInput in) throws IOException {
            super(in);
        }

        public NodesStoreFilesMetadataBatch(
            ClusterName clusterName,
            List<NodeStoreFilesMetadataBatch> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeStoreFilesMetadataBatch> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeStoreFilesMetadataBatch::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeStoreFilesMetadataBatch> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The metadata for the node store files
     *
     * @opensearch.internal
     */
    public static class NodeStoreFilesMetadata {

        private StoreFilesMetadata storeFilesMetadata;
        private Exception storeFileFetchException;

        public NodeStoreFilesMetadata(StoreFilesMetadata storeFilesMetadata) {
            this.storeFilesMetadata = storeFilesMetadata;
            this.storeFileFetchException = null;
        }

        public NodeStoreFilesMetadata(StreamInput in) throws IOException {
            storeFilesMetadata = new StoreFilesMetadata(in);
            if (in.readBoolean()) {
                this.storeFileFetchException = in.readException();
            } else {
                this.storeFileFetchException = null;
            }
        }

        public NodeStoreFilesMetadata(StoreFilesMetadata storeFilesMetadata, Exception storeFileFetchException) {
            this.storeFilesMetadata = storeFilesMetadata;
            this.storeFileFetchException = storeFileFetchException;
        }

        public StoreFilesMetadata storeFilesMetadata() {
            return storeFilesMetadata;
        }

        public static NodeStoreFilesMetadata readListShardStoreNodeOperationResponse(StreamInput in) throws IOException {
            return new NodeStoreFilesMetadata(in);
        }

        public void writeTo(StreamOutput out) throws IOException {
            storeFilesMetadata.writeTo(out);
            if (storeFileFetchException != null) {
                out.writeBoolean(true);
                out.writeException(storeFileFetchException);
            } else {
                out.writeBoolean(false);
            }
        }

        public Exception getStoreFileFetchException() {
            return storeFileFetchException;
        }

        @Override
        public String toString() {
            return "[[" + storeFilesMetadata + "]]";
        }
    }

    /**
     * NodeRequest class is for deserializing the  request received by this node from other node for this transport action.
     * This is used in {@link TransportNodesAction}
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {

        private final Map<ShardId, String> shardIdsWithCustomDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardIdsWithCustomDataPath = in.readMap(ShardId::new, StreamInput::readString);
        }

        public NodeRequest(Request request) {
            this.shardIdsWithCustomDataPath = Objects.requireNonNull(request.getShardIdsWithCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdsWithCustomDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }

        public Map<ShardId, String> getShardIdsWithCustomDataPath() {
            return shardIdsWithCustomDataPath;
        }
    }

    /**
     * NodeStoreFilesMetadataBatch Response received by the node from other node for this transport action.
     * Refer {@link TransportNodesAction}
     */
    public static class NodeStoreFilesMetadataBatch extends BaseNodeResponse {
        private final Map<ShardId, NodeStoreFilesMetadata> nodeStoreFilesMetadataBatch;

        protected NodeStoreFilesMetadataBatch(StreamInput in) throws IOException {
            super(in);
            this.nodeStoreFilesMetadataBatch = in.readMap(ShardId::new, NodeStoreFilesMetadata::new);
        }

        public NodeStoreFilesMetadataBatch(DiscoveryNode node, Map<ShardId, NodeStoreFilesMetadata> nodeStoreFilesMetadataBatch) {
            super(node);
            this.nodeStoreFilesMetadataBatch = nodeStoreFilesMetadataBatch;
        }

        public Map<ShardId, NodeStoreFilesMetadata> getNodeStoreFilesMetadataBatch() {
            return this.nodeStoreFilesMetadataBatch;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(nodeStoreFilesMetadataBatch, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
        }
    }

}
