/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.list;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transport action that returns a page of indices within a snapshot with summary stats.
 *
 * @opensearch.internal
 */
public class TransportSnapshotIndicesListAction extends TransportClusterManagerNodeAction<SnapshotIndicesListRequest, SnapshotIndicesListResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportSnapshotIndicesListAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RepositoriesService repositoriesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            SnapshotIndicesListAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            TransportSnapshotIndicesListAction::newRequest,
            indexNameExpressionResolver
        );
        this.repositoriesService = repositoriesService;
    }

    private static SnapshotIndicesListRequest newRequest(StreamInput in) throws IOException {
        return new SnapshotIndicesListRequest(in);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected ClusterBlockException checkBlock(SnapshotIndicesListRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected SnapshotIndicesListResponse read(StreamInput in) throws IOException {
        return new SnapshotIndicesListResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        SnapshotIndicesListRequest request,
        ClusterState state,
        ActionListener<SnapshotIndicesListResponse> listener
    ) throws Exception {
        final String repository = Objects.requireNonNull(request.repository(), "repository is required");
        final String snapshotName = Objects.requireNonNull(request.snapshot(), "snapshot is required");
        final int from = Math.max(0, request.from());
        final int size = Math.max(0, request.size());

        repositoriesService.getRepositoryData(repository, ActionListener.wrap(repositoryData -> {
            final SnapshotId snapshotId = repositoryData.getSnapshotIds()
                .stream()
                .filter(s -> s.getName().equals(snapshotName))
                .findFirst()
                .orElse(null);
            if (snapshotId == null) {
                listener.onResponse(new SnapshotIndicesListResponse());
                return;
            }
            final SnapshotInfo snapshotInfo = repositoriesService.repository(repository).getSnapshotInfo(snapshotId);
            // Order indices deterministically (by name)
            final List<String> allIndices = snapshotInfo.indices().stream().sorted().collect(Collectors.toList());
            final int start = Math.min(from, allIndices.size());
            final int end = Math.min(start + size, allIndices.size());
            final List<String> page = new ArrayList<>(allIndices.subList(start, end));

            fetchIndexRows(repository, repositoryData, snapshotInfo, page, ActionListener.wrap(rows -> {
                SnapshotIndicesListResponse resp = new SnapshotIndicesListResponse();
                resp.rows().addAll(rows);
                listener.onResponse(resp);
            }, listener::onFailure));
        }, listener::onFailure));
    }

    private void fetchIndexRows(
        String repository,
        RepositoryData repositoryData,
        SnapshotInfo snapshotInfo,
        List<String> pageIndices,
        ActionListener<List<SnapshotIndicesListResponse.IndexRow>> listener
    ) {
        try {
            Repository repo = repositoriesService.repository(repository);
            Map<String, org.opensearch.cluster.metadata.IndexMetadata> indexMetadataMap = SnapshotListUtils.loadIndexMetadata(
                repo,
                repositoryData,
                snapshotInfo,
                pageIndices
            );
            Map<String, SnapshotListUtils.IndexFileStats> fileStats = SnapshotListUtils.loadIndexFileStats(
                repo,
                repositoryData,
                snapshotInfo,
                pageIndices,
                indexMetadataMap
            );

            final long startTime = snapshotInfo.startTime();
            final long endTime = snapshotInfo.endTime() == 0 ? threadPool.absoluteTimeInMillis() : snapshotInfo.endTime();
            final long timeInMillis = Math.max(0L, endTime - startTime);

            List<SnapshotIndicesListResponse.IndexRow> rows = new ArrayList<>();
            for (String index : pageIndices) {
                int totalShards = indexMetadataMap.containsKey(index) ? indexMetadataMap.get(index).getNumberOfShards() : 0;
                int failed = (int) snapshotInfo.shardFailures().stream().filter(f -> index.equals(f.index())).count();
                int done = Math.max(0, totalShards - failed);
                SnapshotListUtils.IndexFileStats stats = fileStats.getOrDefault(index, SnapshotListUtils.IndexFileStats.EMPTY);

                SnapshotIndicesListResponse.IndexRow row = new SnapshotIndicesListResponse.IndexRow();
                row.index = index;
                row.shardsTotal = totalShards;
                row.shardsDone = done;
                row.shardsFailed = failed;
                row.fileCount = stats.fileCount;
                row.sizeInBytes = stats.sizeInBytes;
                row.startTimeInMillis = startTime;
                row.timeInMillis = timeInMillis;
                rows.add(row);
            }
            listener.onResponse(rows);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}


