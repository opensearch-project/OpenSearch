/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.PlainShardsIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transport action that collects catalog snapshot data using broadcast-by-node routing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TransportCatalogSnapshotAction extends TransportBroadcastByNodeAction<
    CatalogSnapshotRequest,
    CatalogSnapshotResponse,
    CatalogSnapshotShardResult> {

    private final IndicesService indicesService;

    @Inject
    public TransportCatalogSnapshotAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            CatalogSnapshotActionType.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            CatalogSnapshotRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    @Override
    protected CatalogSnapshotShardResult readShardResult(StreamInput in) throws IOException {
        return new CatalogSnapshotShardResult(in);
    }

    @Override
    protected CatalogSnapshotResponse newResponse(
        CatalogSnapshotRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<CatalogSnapshotShardResult> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new CatalogSnapshotResponse(results, request.isShardLevel(), totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected CatalogSnapshotRequest readRequestFrom(StreamInput in) throws IOException {
        return new CatalogSnapshotRequest(in);
    }

    @Override
    protected CatalogSnapshotShardResult shardOperation(CatalogSnapshotRequest request, ShardRouting shardRouting) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());

        try (GatedCloseable<CatalogSnapshot> snapshotRef = indexShard.getCatalogSnapshot()) {
            CatalogSnapshot snapshot = snapshotRef.get();
            BytesReference bytes = buildSnapshotXContent(shardRouting.getIndexName(), snapshot);
            return new CatalogSnapshotShardResult(shardRouting, bytes);
        }
    }

    private BytesReference buildSnapshotXContent(String indexName, CatalogSnapshot snapshot) throws IOException {
        try (XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder()) {
            builder.startObject();
            builder.field("index", indexName);
            builder.field("generation", snapshot.getGeneration());
            builder.field("version", snapshot.getVersion());
            builder.field("num_docs", snapshot.getNumDocs());

            List<Segment> segments = snapshot.getSegments();
            Map<String, ExtensionSummary> byExtension = new HashMap<>();
            Map<String, FormatSummary> byFormat = new HashMap<>();

            builder.startArray("segments");
            for (Segment segment : segments) {
                builder.startObject();
                builder.field("generation", segment.generation());
                builder.startObject("formats");
                for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
                    String format = entry.getKey();
                    WriterFileSet wfs = entry.getValue();
                    builder.startObject(format);
                    builder.field("num_rows", wfs.numRows());
                    long formatTotalSize = 0;
                    builder.startArray("files");
                    for (String fileName : wfs.files()) {
                        Path filePath = Path.of(wfs.directory(), fileName);
                        long size = 0;
                        try {
                            if (Files.exists(filePath)) {
                                size = Files.size(filePath);
                            }
                        } catch (IOException ignored) {}
                        builder.startObject();
                        builder.field("name", fileName);
                        builder.field("size_bytes", size);
                        builder.endObject();
                        formatTotalSize += size;
                        String ext = getExtension(fileName);
                        byExtension.computeIfAbsent(ext, k -> new ExtensionSummary()).add(size);
                    }
                    builder.endArray();
                    builder.field("total_size_bytes", formatTotalSize);
                    builder.endObject();
                    byFormat.computeIfAbsent(format, k -> new FormatSummary()).add(wfs.files().size(), formatTotalSize, wfs.numRows());
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();

            builder.startObject("summary");
            builder.field("total_segments", segments.size());
            builder.startObject("by_extension");
            for (Map.Entry<String, ExtensionSummary> entry : byExtension.entrySet()) {
                builder.startObject(entry.getKey());
                builder.field("file_count", entry.getValue().fileCount);
                builder.field("total_size_bytes", entry.getValue().totalSize);
                builder.endObject();
            }
            builder.endObject();
            builder.startObject("by_format");
            for (Map.Entry<String, FormatSummary> entry : byFormat.entrySet()) {
                builder.startObject(entry.getKey());
                builder.field("file_count", entry.getValue().fileCount);
                builder.field("total_size_bytes", entry.getValue().totalSize);
                builder.field("total_rows", entry.getValue().totalRows);
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();

            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private static String getExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot >= 0 ? fileName.substring(dot) : "";
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, CatalogSnapshotRequest request, String[] concreteIndices) {
        List<ShardRouting> shards = clusterState.routingTable().allShards(concreteIndices).getShardRoutings();
        String nodeFilter = request.getNodeFilter();
        Integer shardFilter = request.getShardFilter();

        // Validate shard filter against actual shard count
        if (shardFilter != null) {
            for (String index : concreteIndices) {
                int numShards = clusterState.routingTable().index(index).getShards().size();
                if (shardFilter < 0 || shardFilter >= numShards) {
                    throw new IllegalArgumentException(
                        "shard " + shardFilter + " is out of range; index [" + index + "] has only " + numShards + " shards"
                    );
                }
            }
        }

        // Resolve _local to actual node ID and validate node existence
        String resolvedNodeFilter = null;
        if (nodeFilter != null) {
            if ("_local".equals(nodeFilter)) {
                resolvedNodeFilter = clusterState.getNodes().getLocalNodeId();
            } else {
                if (clusterState.getNodes().get(nodeFilter) == null) {
                    throw new IllegalArgumentException("node [" + nodeFilter + "] not found in cluster");
                }
                resolvedNodeFilter = nodeFilter;
            }
        }

        final String finalNodeFilter = resolvedNodeFilter;
        List<ShardRouting> filtered = shards.stream().filter(sr -> {
            if (shardFilter != null && sr.shardId().id() != shardFilter) {
                return false;
            }
            if (finalNodeFilter != null && !finalNodeFilter.equals(sr.currentNodeId())) {
                return false;
            }
            if (shardFilter != null || request.isShardLevel()) {
                return true;
            }
            return sr.primary();
        }).collect(Collectors.toList());

        return new PlainShardsIterator(filtered);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, CatalogSnapshotRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, CatalogSnapshotRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    private static class ExtensionSummary {
        int fileCount;
        long totalSize;

        void add(long size) {
            fileCount++;
            totalSize += size;
        }
    }

    private static class FormatSummary {
        int fileCount;
        long totalSize;
        long totalRows;

        void add(int files, long size, long rows) {
            fileCount += files;
            totalSize += size;
            totalRows += rows;
        }
    }
}
