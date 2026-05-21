/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.action;

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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action that analyzes parquet files using broadcast-by-node routing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TransportParquetAnalyzeAction extends TransportBroadcastByNodeAction<
    ParquetAnalyzeRequest,
    ParquetAnalyzeResponse,
    ParquetAnalyzeShardResult> {

    private final IndicesService indicesService;

    @Inject
    public TransportParquetAnalyzeAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ParquetAnalyzeActionType.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ParquetAnalyzeRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    @Override
    protected ParquetAnalyzeShardResult readShardResult(StreamInput in) throws IOException {
        return new ParquetAnalyzeShardResult(in);
    }

    @Override
    protected ParquetAnalyzeResponse newResponse(
        ParquetAnalyzeRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ParquetAnalyzeShardResult> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new ParquetAnalyzeResponse(results, request.isShardLevel(), totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ParquetAnalyzeRequest readRequestFrom(StreamInput in) throws IOException {
        return new ParquetAnalyzeRequest(in);
    }

    @Override
    protected ParquetAnalyzeShardResult shardOperation(ParquetAnalyzeRequest request, ShardRouting shardRouting) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());

        long totalRows = 0;
        long totalSizeBytes = 0;
        long totalFooterSize = 0;
        List<Object> sortingColumns = null;
        Map<String, FieldStats> fieldStatsMap = new HashMap<>();
        List<Map<String, Object>> fileDetails = new ArrayList<>();

        try (GatedCloseable<CatalogSnapshot> snapshotRef = indexShard.getCatalogSnapshot()) {
            CatalogSnapshot snapshot = snapshotRef.get();
            for (Segment segment : snapshot.getSegments()) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(ParquetDataFormat.PARQUET_DATA_FORMAT_NAME);
                if (wfs == null) {
                    continue;
                }
                totalRows += wfs.numRows();
                for (String fileName : wfs.files()) {
                    String filePath = Path.of(wfs.directory(), fileName).toString();
                    totalSizeBytes += Files.size(Path.of(filePath));
                    String json = RustBridge.analyzeFile(filePath);
                    Map<String, Object> parsed = parseAnalyzeJson(json);
                    aggregateFields(parsed, fieldStatsMap);
                    totalFooterSize += toLong(parsed.get("footer_size"));
                    if (sortingColumns == null && parsed.get("sorting_columns") instanceof List<?> sc) {
                        sortingColumns = new ArrayList<>(sc);
                    }
                    if (request.isFileLevel()) {
                        Map<String, Object> fileDetail = new HashMap<>();
                        fileDetail.put("file", fileName);
                        fileDetail.put("detail", parsed);
                        fileDetails.add(fileDetail);
                    }
                }
            }
        }

        BytesReference bytes = buildAnalyzeXContent(
            shardRouting.getIndexName(),
            totalRows,
            totalSizeBytes,
            totalFooterSize,
            sortingColumns,
            fieldStatsMap,
            request.isFileLevel(),
            fileDetails
        );
        return new ParquetAnalyzeShardResult(shardRouting, bytes);
    }

    @SuppressWarnings("unchecked")
    private BytesReference buildAnalyzeXContent(
        String indexName,
        long totalRows,
        long totalSizeBytes,
        long totalFooterSize,
        List<Object> sortingColumns,
        Map<String, FieldStats> fieldStatsMap,
        boolean fileLevel,
        List<Map<String, Object>> fileDetails
    ) throws IOException {
        try (XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder()) {
            builder.startObject();
            builder.field("index", indexName);
            builder.field("total_rows", totalRows);
            builder.field("total_size_bytes", totalSizeBytes);
            builder.field("footer_size", totalFooterSize);
            if (sortingColumns != null) {
                builder.field("sorting_columns", sortingColumns);
            }
            buildFieldsArray(builder, fieldStatsMap);
            if (fileLevel) {
                buildFilesArray(builder, fileDetails);
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, ParquetAnalyzeRequest request, String[] concreteIndices) {
        List<ShardRouting> shards = clusterState.routingTable().allShards(concreteIndices).getShardRoutings();
        String nodeFilter = request.getNodeFilter();
        Integer shardFilter = request.getShardFilter();

        // Validate shard filter against actual shard count
        if (shardFilter != null) {
            for (String index : concreteIndices) {
                int numShards = clusterState.routingTable().index(index).getShards().size();
                if (shardFilter < 0 || shardFilter >= numShards) {
                    throw new ShardNotFoundException(new ShardId(clusterState.metadata().index(index).getIndex(), shardFilter));
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
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ParquetAnalyzeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ParquetAnalyzeRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    // --- Helper methods ported from the original ParquetAnalyzeAction ---

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseAnalyzeJson(String json) {
        try {
            return org.opensearch.common.xcontent.XContentHelper.convertToMap(
                org.opensearch.common.xcontent.XContentType.JSON.xContent(),
                json,
                false
            );
        } catch (Exception e) {
            return Map.of();
        }
    }

    @SuppressWarnings("unchecked")
    private static void aggregateFields(Map<String, Object> parsed, Map<String, FieldStats> fieldStatsMap) {
        Object rowGroupsObj = parsed.get("row_groups");
        if (rowGroupsObj instanceof List<?> rowGroups) {
            for (Object rgObj : rowGroups) {
                if (rgObj instanceof Map<?, ?> rg) {
                    Object columnsObj = rg.get("columns");
                    if (columnsObj instanceof List<?> columns) {
                        aggregateColumnsFromList(columns, fieldStatsMap);
                    }
                }
            }
        }
        Object columnsObj = parsed.get("columns");
        if (columnsObj instanceof List<?> columns) {
            aggregateColumnsFromList(columns, fieldStatsMap);
        }
    }

    @SuppressWarnings("unchecked")
    private static void aggregateColumnsFromList(List<?> columns, Map<String, FieldStats> fieldStatsMap) {
        for (Object colObj : columns) {
            if (colObj instanceof Map<?, ?> col) {
                String name = (String) col.get("name");
                if (name == null) continue;
                FieldStats stats = fieldStatsMap.computeIfAbsent(name, k -> new FieldStats());
                Object type = col.get("type");
                if (type != null) stats.type = (String) type;
                Object compression = col.get("compression");
                if (compression != null) stats.compression = (String) compression;
                if (col.get("encodings") instanceof List<?> encodings) {
                    for (Object enc : encodings) {
                        stats.encodings.add(String.valueOf(enc));
                    }
                }
                stats.totalCompressedBytes += toLong(col.get("compressed_bytes"));
                stats.totalUncompressedBytes += toLong(col.get("uncompressed_bytes"));
                stats.nullCount += toLong(col.get("null_count"));
                stats.numValues += toLong(col.get("num_values"));
                if (Boolean.TRUE.equals(col.get("has_bloom_filter"))) {
                    stats.hasBloomFilter = true;
                }
                stats.bloomFilterSize += toLong(col.get("bloom_filter_size"));
                if (col.get("stats") instanceof Map<?, ?> colStats) {
                    String min = (String) colStats.get("min");
                    String max = (String) colStats.get("max");
                    if (min != null && (stats.statsMin == null || min.compareTo(stats.statsMin) < 0)) {
                        stats.statsMin = min;
                    }
                    if (max != null && (stats.statsMax == null || max.compareTo(stats.statsMax) > 0)) {
                        stats.statsMax = max;
                    }
                    stats.statsNullCount += toLong(colStats.get("null_count"));
                    Object distinctObj = colStats.get("distinct_count");
                    if (distinctObj == null) {
                        stats.distinctCountIsNull = true;
                    } else if (!stats.distinctCountIsNull) {
                        stats.statsDistinctCount += toLong(distinctObj);
                    }
                }
                if (col.get("page_stats") instanceof Map<?, ?> pageStats) {
                    stats.totalNumPages += toLong(pageStats.get("num_pages"));
                }
            }
        }
    }

    private static long toLong(Object obj) {
        if (obj instanceof Number n) {
            return n.longValue();
        }
        return 0L;
    }

    private static void buildFieldsArray(XContentBuilder builder, Map<String, FieldStats> fieldStatsMap) throws IOException {
        builder.startArray("fields");
        for (Map.Entry<String, FieldStats> entry : fieldStatsMap.entrySet()) {
            FieldStats stats = entry.getValue();
            builder.startObject();
            builder.field("name", entry.getKey());
            builder.field("type", stats.type);
            builder.field("compression", stats.compression);
            builder.array("encodings", stats.encodings.toArray(new String[0]));
            builder.field("total_compressed_bytes", stats.totalCompressedBytes);
            builder.field("total_uncompressed_bytes", stats.totalUncompressedBytes);
            double ratio = stats.totalCompressedBytes > 0 ? (double) stats.totalUncompressedBytes / stats.totalCompressedBytes : 0.0;
            builder.field("compression_ratio", Math.round(ratio * 100.0) / 100.0);
            builder.field("null_count", stats.nullCount);
            builder.field("num_values", stats.numValues);
            builder.field("has_bloom_filter", stats.hasBloomFilter);
            builder.field("bloom_filter_size", stats.bloomFilterSize);
            builder.startObject("stats");
            builder.field("min", stats.statsMin);
            builder.field("max", stats.statsMax);
            builder.field("null_count", stats.statsNullCount);
            if (stats.distinctCountIsNull) {
                builder.nullField("distinct_count");
            } else {
                builder.field("distinct_count", stats.statsDistinctCount);
            }
            builder.endObject();
            builder.field("total_num_pages", stats.totalNumPages);
            builder.endObject();
        }
        builder.endArray();
    }

    @SuppressWarnings("unchecked")
    private static void buildFilesArray(XContentBuilder builder, List<Map<String, Object>> fileDetails) throws IOException {
        builder.startArray("files");
        for (Map<String, Object> fileDetail : fileDetails) {
            builder.startObject();
            builder.field("file", fileDetail.get("file"));
            Object detail = fileDetail.get("detail");
            if (detail instanceof Map<?, ?> detailMap) {
                if (detailMap.containsKey("footer_size")) {
                    builder.field("footer_size", detailMap.get("footer_size"));
                }
                if (detailMap.get("sorting_columns") instanceof List<?> sc) {
                    builder.field("sorting_columns", sc);
                }
                Object rowGroups = detailMap.get("row_groups");
                if (rowGroups instanceof List<?> rgs) {
                    builder.startArray("row_groups");
                    for (Object rg : rgs) {
                        builder.value(rg);
                    }
                    builder.endArray();
                }
            }
            builder.endObject();
        }
        builder.endArray();
    }

    private static class FieldStats {
        String type = "UNKNOWN";
        String compression = "UNKNOWN";
        Set<String> encodings = new HashSet<>();
        long totalCompressedBytes;
        long totalUncompressedBytes;
        long nullCount;
        long numValues;
        boolean hasBloomFilter;
        long bloomFilterSize;
        String statsMin;
        String statsMax;
        long statsNullCount;
        long statsDistinctCount;
        boolean distinctCountIsNull;
        long totalNumPages;
    }
}
