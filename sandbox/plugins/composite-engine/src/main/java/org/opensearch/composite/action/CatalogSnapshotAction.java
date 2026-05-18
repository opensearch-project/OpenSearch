/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.composite.stats.CompositeStatsRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST handler for {@code GET /_plugins/composite/{index}/_catalog_snapshot}.
 * <p>
 * Returns a file-level breakdown of the current {@link CatalogSnapshot} by format and extension.
 * Supports optional {@code shard} query parameter to filter to a specific shard.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CatalogSnapshotAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "catalog_snapshot_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_plugins/composite/{index}/_catalog_snapshot"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String indexName = request.param("index");
        String shardParam = request.param("shard");
        Integer shardFilter = shardParam != null ? Integer.parseInt(shardParam) : null;

        return channel -> {
            try {
                CompositeStatsRegistry registry = CompositeStatsRegistry.getInstance();
                IndicesService indicesService = registry.getIndicesService();
                if (indicesService == null) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.SERVICE_UNAVAILABLE, "IndicesService not available"));
                    return;
                }

                // Find matching shards from the registry
                Map<ShardId, Object> matchingShards = new HashMap<>();
                for (ShardId shardId : registry.getEngines().keySet()) {
                    if (shardId.getIndexName().equals(indexName)) {
                        if (shardFilter == null || shardId.id() == shardFilter) {
                            matchingShards.put(shardId, null);
                        }
                    }
                }

                if (matchingShards.isEmpty()) {
                    channel.sendResponse(
                        new BytesRestResponse(RestStatus.NOT_FOUND, "No composite engine found for index [" + indexName + "]")
                    );
                    return;
                }

                // Use the first matching shard to get the catalog snapshot
                ShardId targetShardId = matchingShards.keySet().iterator().next();
                IndexService indexService = indicesService.indexServiceSafe(targetShardId.getIndex());
                IndexShard indexShard = indexService.getShard(targetShardId.id());

                try (GatedCloseable<CatalogSnapshot> snapshotRef = indexShard.getCatalogSnapshot()) {
                    CatalogSnapshot snapshot = snapshotRef.get();
                    XContentBuilder builder = channel.newBuilder();
                    buildResponse(builder, indexName, snapshot);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            } catch (Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }

    private void buildResponse(XContentBuilder builder, String indexName, CatalogSnapshot snapshot) throws IOException {
        List<Segment> segments = snapshot.getSegments();

        // Summary accumulators
        Map<String, ExtensionSummary> byExtension = new HashMap<>();
        Map<String, FormatSummary> byFormat = new HashMap<>();

        builder.startObject();
        builder.field("index", indexName);
        builder.field("generation", snapshot.getGeneration());
        builder.field("version", snapshot.getVersion());
        builder.field("num_docs", snapshot.getNumDocs());

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

                    // Track by extension
                    String ext = getExtension(fileName);
                    byExtension.computeIfAbsent(ext, k -> new ExtensionSummary()).add(size);
                }
                builder.endArray();
                builder.field("total_size_bytes", formatTotalSize);
                builder.endObject();

                // Track by format
                byFormat.computeIfAbsent(format, k -> new FormatSummary()).add(wfs.files().size(), formatTotalSize, wfs.numRows());
            }

            builder.endObject(); // formats
            builder.endObject(); // segment
        }
        builder.endArray(); // segments

        // Summary
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

        builder.endObject(); // summary
        builder.endObject(); // root
    }

    private static String getExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot >= 0 ? fileName.substring(dot) : "";
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
