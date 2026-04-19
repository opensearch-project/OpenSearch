/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.status;

import org.opensearch.action.ActionRunnable;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotMissingException;
import org.opensearch.snapshots.SnapshotShardFailure;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.snapshots.SnapshotsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transport action for the paginated snapshot index status API.
 *
 * <p>For <b>completed</b> snapshots only the requested page of indices is fetched from the
 * repository, reducing the number of blocking I/O calls from O(total-indices + total-shards) to
 * O(page-indices + page-shards).
 *
 * <p>For <b>running</b> snapshots the pagination parameters have no effect: all index statuses are
 * returned using the same in-memory path as {@link TransportSnapshotsStatusAction}.
 *
 * @opensearch.internal
 */
public class TransportSnapshotIndexStatusAction extends TransportClusterManagerNodeAction<
    SnapshotIndexStatusRequest,
    SnapshotIndexStatusResponse> {

    private final RepositoriesService repositoriesService;
    private final TransportNodesSnapshotsStatus transportNodesSnapshotsStatus;

    @Inject
    public TransportSnapshotIndexStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        TransportNodesSnapshotsStatus transportNodesSnapshotsStatus,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            SnapshotIndexStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SnapshotIndexStatusRequest::new,
            indexNameExpressionResolver
        );
        this.repositoriesService = repositoriesService;
        this.transportNodesSnapshotsStatus = transportNodesSnapshotsStatus;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected ClusterBlockException checkBlock(SnapshotIndexStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected SnapshotIndexStatusResponse read(StreamInput in) throws IOException {
        return new SnapshotIndexStatusResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        final SnapshotIndexStatusRequest request,
        final ClusterState state,
        final ActionListener<SnapshotIndexStatusResponse> listener
    ) throws Exception {
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);

        // Check if the requested snapshot is currently running.
        List<SnapshotsInProgress.Entry> currentEntries = SnapshotsService.currentSnapshots(
            snapshotsInProgress,
            request.repository(),
            Collections.singletonList(request.snapshot())
        );

        if (!currentEntries.isEmpty()) {
            // Snapshot is running — return full status without pagination (already fast via in-memory maps).
            handleRunningSnapshot(request, currentEntries.get(0), listener);
        } else {
            // Snapshot is completed — use the paginated repository path.
            handleCompletedSnapshot(request, listener);
        }
    }

    // -----------------------------------------------------------------------
    // Running snapshot path
    // -----------------------------------------------------------------------

    private void handleRunningSnapshot(
        SnapshotIndexStatusRequest request,
        SnapshotsInProgress.Entry entry,
        ActionListener<SnapshotIndexStatusResponse> listener
    ) {
        // Collect node IDs that hold in-progress shards so we can query them for detailed stats.
        Set<String> nodeIds = new HashSet<>();
        for (Map.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : entry.shards().entrySet()) {
            if (shardEntry.getValue().nodeId() != null) {
                nodeIds.add(shardEntry.getValue().nodeId());
            }
        }

        if (nodeIds.isEmpty()) {
            // No in-progress shards on any node — build response from cluster state alone.
            threadPool.generic()
                .execute(
                    ActionRunnable.wrap(
                        listener,
                        l -> l.onResponse(buildRunningSnapshotResponse(entry, Collections.emptyMap()))
                    )
                );
            return;
        }

        transportNodesSnapshotsStatus.execute(
            new TransportNodesSnapshotsStatus.Request(nodeIds.toArray(Strings.EMPTY_ARRAY)).snapshots(
                new Snapshot[] { entry.snapshot() }
            ).timeout(request.clusterManagerNodeTimeout()),
            ActionListener.wrap(
                nodeStatuses -> threadPool.generic()
                    .execute(
                        ActionRunnable.wrap(
                            listener,
                            l -> l.onResponse(buildRunningSnapshotResponse(entry, nodeStatuses.getNodesMap()))
                        )
                    ),
                listener::onFailure
            )
        );
    }

    private SnapshotIndexStatusResponse buildRunningSnapshotResponse(
        SnapshotsInProgress.Entry entry,
        Map<String, TransportNodesSnapshotsStatus.NodeSnapshotStatus> nodeSnapshotStatusMap
    ) {
        List<SnapshotIndexShardStatus> shardStatusList = new ArrayList<>();

        // Lazy-built lookup map from index name to IndexId — only needed for DONE shards that
        // are not yet confirmed in cluster state, so we can call getShardSnapshotStatus().
        Map<String, IndexId> indexIdLookup = null;

        for (Map.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : entry.shards().entrySet()) {
            SnapshotsInProgress.ShardSnapshotStatus status = shardEntry.getValue();

            if (status.nodeId() != null) {
                TransportNodesSnapshotsStatus.NodeSnapshotStatus nodeStatus = nodeSnapshotStatusMap.get(status.nodeId());
                if (nodeStatus != null) {
                    Map<ShardId, SnapshotIndexShardStatus> shardStatuses = nodeStatus.status().get(entry.snapshot());
                    if (shardStatuses != null) {
                        SnapshotIndexShardStatus shardStatus = shardStatuses.get(shardEntry.getKey());
                        if (shardStatus != null) {
                            if (shardStatus.getStage() == SnapshotIndexShardStage.DONE
                                && shardEntry.getValue().state() != SnapshotsInProgress.ShardState.SUCCESS) {
                                shardStatus = new SnapshotIndexShardStatus(
                                    shardEntry.getKey(),
                                    SnapshotIndexShardStage.FINALIZE,
                                    shardStatus.getStats(),
                                    shardStatus.getNodeId(),
                                    shardStatus.getFailure()
                                );
                            }
                            shardStatusList.add(shardStatus);
                            continue;
                        }
                    }
                }
            }

            // Node data unavailable — derive stage from cluster state.
            final SnapshotIndexShardStage stage;
            switch (status.state()) {
                case FAILED:
                case ABORTED:
                case MISSING:
                    stage = SnapshotIndexShardStage.FAILURE;
                    break;
                case INIT:
                case WAITING:
                case QUEUED:
                    stage = SnapshotIndexShardStage.STARTED;
                    break;
                case SUCCESS:
                    stage = SnapshotIndexShardStage.DONE;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown snapshot shard state " + status.state());
            }

            if (stage == SnapshotIndexShardStage.DONE) {
                // Shard is done but node hasn't confirmed yet — fetch actual stats from repository.
                if (indexIdLookup == null) {
                    indexIdLookup = entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity()));
                }
                final ShardId shardId = shardEntry.getKey();
                shardStatusList.add(
                    new SnapshotIndexShardStatus(
                        shardId,
                        repositoriesService.repository(entry.repository())
                            .getShardSnapshotStatus(entry.snapshot().getSnapshotId(), indexIdLookup.get(shardId.getIndexName()), shardId)
                            .asCopy()
                    )
                );
            } else {
                shardStatusList.add(new SnapshotIndexShardStatus(shardEntry.getKey(), stage));
            }
        }

        // Count distinct indices in this snapshot for the `total` field.
        Set<String> distinctIndices = new HashSet<>();
        for (ShardId shardId : entry.shards().keySet()) {
            distinctIndices.add(shardId.getIndexName());
        }

        return new SnapshotIndexStatusResponse(
            entry.state(),
            distinctIndices.size(),
            0,
            distinctIndices.size(),
            shardStatusList
        );
    }

    // -----------------------------------------------------------------------
    // Completed snapshot path (paginated)
    // -----------------------------------------------------------------------

    private void handleCompletedSnapshot(
        SnapshotIndexStatusRequest request,
        ActionListener<SnapshotIndexStatusResponse> listener
    ) {
        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        repositoriesService.getRepositoryData(request.repository(), repositoryDataListener);

        repositoryDataListener.whenComplete(repositoryData -> {
            // Resolve the snapshot ID from the repository.
            SnapshotId snapshotId = repositoryData.getSnapshotIds()
                .stream()
                .filter(s -> s.getName().equals(request.snapshot()))
                .findFirst()
                .orElseThrow(() -> new SnapshotMissingException(request.repository(), request.snapshot()));

            final Repository repository = repositoriesService.repository(request.repository());
            final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);

            // Map snapshot state to SnapshotsInProgress.State for the response.
            final SnapshotsInProgress.State responseState = mapSnapshotState(snapshotInfo.state());

            final List<String> allIndices = snapshotInfo.indices(); // deterministic ordered list
            final int totalIndices = allIndices.size();

            if (totalIndices == 0 || request.from() >= totalIndices) {
                // Nothing to return for this page.
                listener.onResponse(
                    new SnapshotIndexStatusResponse(responseState, totalIndices, request.from(), request.size(), Collections.emptyList())
                );
                return;
            }

            // Slice the page.
            final int pageEnd = Math.min(request.from() + request.size(), totalIndices);
            final List<String> pageIndices = allIndices.subList(request.from(), pageEnd);

            // Fetch shard statuses only for the page slice.
            final List<SnapshotIndexShardStatus> shardStatusList = fetchShardStatusesForPage(
                repository,
                repositoryData,
                snapshotInfo,
                pageIndices
            );

            listener.onResponse(
                new SnapshotIndexStatusResponse(responseState, totalIndices, request.from(), request.size(), shardStatusList)
            );
        }, listener::onFailure);
    }

    /**
     * Fetches index metadata and shard snapshot statuses for the given slice of index names.
     * Only {@code pageIndices.size()} index metadata calls and the corresponding shard status
     * calls are made — O(page-indices + page-shards) instead of O(total-indices + total-shards).
     */
    private List<SnapshotIndexShardStatus> fetchShardStatusesForPage(
        Repository repository,
        RepositoryData repositoryData,
        SnapshotInfo snapshotInfo,
        List<String> pageIndices
    ) throws IOException {
        final boolean isV2Snapshot = snapshotInfo.getPinnedTimestamp() > 0;

        // Fetch index metadata for each index in the page.
        final Map<IndexId, IndexMetadata> indexMetadataMap = new HashMap<>(pageIndices.size());
        for (String indexName : pageIndices) {
            IndexId indexId = repositoryData.resolveIndexId(indexName);
            IndexMetadata indexMetadata = repository.getSnapshotIndexMetaData(repositoryData, snapshotInfo.snapshotId(), indexId);
            if (indexMetadata != null) {
                indexMetadataMap.put(indexId, indexMetadata);
            }
        }

        // Build a lookup map for shard failures so we can avoid extra calls per shard.
        final Map<String, Map<Integer, SnapshotShardFailure>> failuresByIndex = buildShardFailureLookup(snapshotInfo);

        final List<SnapshotIndexShardStatus> shardStatusList = new ArrayList<>();
        for (Map.Entry<IndexId, IndexMetadata> entry : indexMetadataMap.entrySet()) {
            final IndexId indexId = entry.getKey();
            final IndexMetadata indexMetadata = entry.getValue();
            final int numberOfShards = indexMetadata.getNumberOfShards();
            final Map<Integer, SnapshotShardFailure> indexFailures = failuresByIndex.getOrDefault(indexId.getName(), Collections.emptyMap());

            for (int i = 0; i < numberOfShards; i++) {
                final ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                final SnapshotShardFailure shardFailure = indexFailures.get(i);

                final IndexShardSnapshotStatus shardSnapshotStatus;
                if (shardFailure != null) {
                    shardSnapshotStatus = IndexShardSnapshotStatus.newFailed(shardFailure.reason());
                } else if (snapshotInfo.state() == SnapshotState.FAILED) {
                    // Partial snapshots were disabled; shard data may not exist.
                    shardSnapshotStatus = IndexShardSnapshotStatus.newFailed("skipped");
                } else if (isV2Snapshot) {
                    shardSnapshotStatus = IndexShardSnapshotStatus.newDone(0, 0, 0, 0, 0, 0, null);
                } else {
                    shardSnapshotStatus = repository.getShardSnapshotStatus(snapshotInfo.snapshotId(), indexId, shardId);
                }

                shardStatusList.add(new SnapshotIndexShardStatus(shardId, shardSnapshotStatus.asCopy()));
            }
        }

        return shardStatusList;
    }

    /**
     * Builds a two-level lookup: index name -> shard id -> {@link SnapshotShardFailure}.
     * This avoids an O(n) linear scan through the failure list for every shard.
     */
    private static Map<String, Map<Integer, SnapshotShardFailure>> buildShardFailureLookup(SnapshotInfo snapshotInfo) {
        if (snapshotInfo.shardFailures().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Map<Integer, SnapshotShardFailure>> lookup = new HashMap<>();
        for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
            lookup.computeIfAbsent(failure.index(), k -> new HashMap<>()).put(failure.shardId(), failure);
        }
        return lookup;
    }

    private static SnapshotsInProgress.State mapSnapshotState(SnapshotState snapshotState) {
        switch (snapshotState) {
            case SUCCESS:
                return SnapshotsInProgress.State.SUCCESS;
            case FAILED:
                return SnapshotsInProgress.State.FAILED;
            case PARTIAL:
                return SnapshotsInProgress.State.PARTIAL;
            default:
                throw new IllegalArgumentException("Unexpected completed snapshot state: " + snapshotState);
        }
    }
}
