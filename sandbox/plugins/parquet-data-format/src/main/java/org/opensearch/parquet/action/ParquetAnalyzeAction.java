/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.action;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * REST handler for {@code GET /_plugins/parquet/{index}/_analyze}.
 * Returns column-level statistics for parquet files in an index.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetAnalyzeAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "parquet_analyze_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_plugins/parquet/{index}/_analyze"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String indexName = request.param("index");
        String shardParam = request.param("shard");
        Integer shardFilter = shardParam != null ? Integer.parseInt(shardParam) : null;
        boolean fileLevel = request.paramAsBoolean("file_level", false);

        return channel -> {
            try {
                IndicesService indicesService = ParquetRegistryInitializer.getIndicesService();
                if (indicesService == null) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.SERVICE_UNAVAILABLE, "IndicesService not available"));
                    return;
                }

                IndexService indexService = findIndexService(indicesService, indexName);
                if (indexService == null) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, "Index [" + indexName + "] not found"));
                    return;
                }

                long totalRows = 0;
                long totalSizeBytes = 0;
                long totalFooterSize = 0;
                List<Object> sortingColumns = null;
                Map<String, FieldStats> fieldStatsMap = new HashMap<>();
                List<Map<String, Object>> fileDetails = new ArrayList<>();

                for (IndexShard shard : indexService) {
                    if (shardFilter != null && shard.shardId().id() != shardFilter) {
                        continue;
                    }
                    try (GatedCloseable<CatalogSnapshot> snapshotRef = shard.getCatalogSnapshot()) {
                        CatalogSnapshot snapshot = snapshotRef.get();
                        for (Segment segment : snapshot.getSegments()) {
                            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(ParquetDataFormat.PARQUET_DATA_FORMAT_NAME);
                            if (wfs == null) {
                                continue;
                            }
                            totalRows += wfs.numRows();
                            for (String fileName : wfs.files()) {
                                String filePath = Path.of(wfs.directory(), fileName).toString();
                                totalSizeBytes += java.nio.file.Files.size(Path.of(filePath));
                                String json = RustBridge.analyzeFile(filePath);
                                Map<String, Object> parsed = parseAnalyzeJson(json);
                                aggregateFields(parsed, fieldStatsMap);
                                totalFooterSize += toLong(parsed.get("footer_size"));
                                // Take sorting_columns from the first file
                                if (sortingColumns == null && parsed.get("sorting_columns") instanceof List<?> sc) {
                                    sortingColumns = new ArrayList<>(sc);
                                }
                                if (fileLevel) {
                                    Map<String, Object> fileDetail = new HashMap<>();
                                    fileDetail.put("file", fileName);
                                    fileDetail.put("detail", parsed);
                                    fileDetails.add(fileDetail);
                                }
                            }
                        }
                    }
                }

                XContentBuilder builder = channel.newBuilder();
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
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }

    private IndexService findIndexService(IndicesService indicesService, String indexName) {
        for (IndexService indexService : indicesService) {
            if (indexService.index().getName().equals(indexName)) {
                return indexService;
            }
        }
        return null;
    }

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
        // Aggregate columns from row_groups (the enhanced Rust output nests columns inside row_groups)
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
        // Also handle flat "columns" key for backward compatibility
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
                // Bloom filter aggregation
                if (Boolean.TRUE.equals(col.get("has_bloom_filter"))) {
                    stats.hasBloomFilter = true;
                }
                stats.bloomFilterSize += toLong(col.get("bloom_filter_size"));
                // Stats aggregation (min/max/null_count/distinct_count)
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
                // Page stats — aggregate total num_pages
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
            // Stats object with merged min/max/null_count/distinct_count
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
                // Include footer_size per file
                if (detailMap.containsKey("footer_size")) {
                    builder.field("footer_size", detailMap.get("footer_size"));
                }
                // Include sorting_columns per file
                if (detailMap.get("sorting_columns") instanceof List<?> sc) {
                    builder.field("sorting_columns", sc);
                }
                // Include full row_groups with page_stats
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
        // Stats aggregation
        String statsMin;
        String statsMax;
        long statsNullCount;
        Long statsDistinctCount = 0L;
        boolean distinctCountIsNull;
        long totalNumPages;
    }
}
