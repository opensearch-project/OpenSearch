/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Response for catalog snapshot containing per-shard results.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CatalogSnapshotResponse extends BroadcastResponse {

    private final List<CatalogSnapshotShardResult> shardResults;
    private final boolean shardLevel;

    public CatalogSnapshotResponse(
        List<CatalogSnapshotShardResult> shardResults,
        boolean shardLevel,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardResults = shardResults;
        this.shardLevel = shardLevel;
    }

    public CatalogSnapshotResponse(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new CatalogSnapshotShardResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (CatalogSnapshotShardResult result : shardResults) {
            result.writeTo(out);
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        if (shardLevel) {
            builder.startArray("shards");
            for (CatalogSnapshotShardResult result : shardResults) {
                builder.startObject();
                builder.field("shard", result.getShardRouting().shardId().id());
                builder.field("primary", result.getShardRouting().primary());
                // Inline the snapshot fields directly into the shard object
                inlineShardResultFields(builder, result.getSnapshotBytes());
                builder.endObject();
            }
            builder.endArray();
        } else {
            // Index-level: aggregate across all primary shard results
            if (!shardResults.isEmpty()) {
                List<CatalogSnapshotShardResult> primaries = shardResults.stream()
                    .filter(r -> r.getShardRouting().primary())
                    .collect(java.util.stream.Collectors.toList());
                if (primaries.isEmpty()) {
                    primaries = shardResults;
                }
                if (primaries.size() == 1) {
                    inlineShardResultFields(builder, primaries.get(0).getSnapshotBytes());
                } else {
                    inlineAggregatedResults(builder, primaries);
                }
            }
        }
    }

    /**
     * Aggregates multiple shard results by merging segments arrays and summing summary counts.
     */
    @SuppressWarnings("unchecked")
    private static void inlineAggregatedResults(XContentBuilder builder, List<CatalogSnapshotShardResult> results) throws IOException {
        // Parse all shard results and merge
        List<Map<String, Object>> parsedResults = new ArrayList<>();
        for (CatalogSnapshotShardResult result : results) {
            Map<String, Object> parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                result.getSnapshotBytes(),
                false,
                XContentType.JSON
            ).v2();
            parsedResults.add(parsed);
        }

        // Use first result as base for scalar fields
        Map<String, Object> first = parsedResults.get(0);
        builder.field("index", first.get("index"));
        builder.field("generation", first.get("generation"));
        builder.field("version", first.get("version"));

        // Sum num_docs across shards
        long totalDocs = 0;
        for (Map<String, Object> p : parsedResults) {
            totalDocs += ((Number) p.getOrDefault("num_docs", 0)).longValue();
        }
        builder.field("num_docs", totalDocs);

        // Merge segments arrays
        List<Object> allSegments = new ArrayList<>();
        for (Map<String, Object> p : parsedResults) {
            Object segs = p.get("segments");
            if (segs instanceof List<?>) {
                allSegments.addAll((List<Object>) segs);
            }
        }
        builder.field("segments");
        builder.value(allSegments);

        // Merge summary: sum by_extension and by_format counts
        builder.startObject("summary");
        builder.field("total_segments", allSegments.size());

        // Aggregate by_extension
        java.util.Map<String, long[]> extAgg = new java.util.LinkedHashMap<>();
        java.util.Map<String, long[]> fmtAgg = new java.util.LinkedHashMap<>();
        for (Map<String, Object> p : parsedResults) {
            Map<String, Object> summary = (Map<String, Object>) p.get("summary");
            if (summary == null) continue;
            Map<String, Object> byExt = (Map<String, Object>) summary.get("by_extension");
            if (byExt != null) {
                for (Map.Entry<String, Object> entry : byExt.entrySet()) {
                    Map<String, Object> val = (Map<String, Object>) entry.getValue();
                    long[] agg = extAgg.computeIfAbsent(entry.getKey(), k -> new long[2]);
                    agg[0] += ((Number) val.getOrDefault("file_count", 0)).longValue();
                    agg[1] += ((Number) val.getOrDefault("total_size_bytes", 0)).longValue();
                }
            }
            Map<String, Object> byFmt = (Map<String, Object>) summary.get("by_format");
            if (byFmt != null) {
                for (Map.Entry<String, Object> entry : byFmt.entrySet()) {
                    Map<String, Object> val = (Map<String, Object>) entry.getValue();
                    long[] agg = fmtAgg.computeIfAbsent(entry.getKey(), k -> new long[3]);
                    agg[0] += ((Number) val.getOrDefault("file_count", 0)).longValue();
                    agg[1] += ((Number) val.getOrDefault("total_size_bytes", 0)).longValue();
                    agg[2] += ((Number) val.getOrDefault("total_rows", 0)).longValue();
                }
            }
        }

        builder.startObject("by_extension");
        for (Map.Entry<String, long[]> entry : extAgg.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field("file_count", entry.getValue()[0]);
            builder.field("total_size_bytes", entry.getValue()[1]);
            builder.endObject();
        }
        builder.endObject();

        builder.startObject("by_format");
        for (Map.Entry<String, long[]> entry : fmtAgg.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field("file_count", entry.getValue()[0]);
            builder.field("total_size_bytes", entry.getValue()[1]);
            builder.field("total_rows", entry.getValue()[2]);
            builder.endObject();
        }
        builder.endObject();

        builder.endObject(); // end summary
    }

    /**
     * Inlines the fields of a shard result's XContent directly into the response builder,
     * instead of wrapping them under a parent key. This keeps the REST response shape flat
     * (e.g., `segments` and `summary` at top level) per the documented contract in HANDOFF.md §2.2.
     */
    private static void inlineShardResultFields(XContentBuilder builder, BytesReference bytes) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, BytesReference.toBytes(bytes))
        ) {
            parser.nextToken(); // START_OBJECT
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                builder.field(fieldName);
                parser.nextToken();
                builder.copyCurrentStructure(parser);
            }
        }
    }
}
