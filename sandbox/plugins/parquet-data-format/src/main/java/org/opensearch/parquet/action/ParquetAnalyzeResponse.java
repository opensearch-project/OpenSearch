/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.action;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Response for parquet analyze containing per-shard results.
 * Index-level aggregation sums numeric leaves across primary shards, with special handling:
 * - {@code distinct_count} is excluded (non-additive across shards; use {@code ?level=shards} to see per-shard values)
 * - {@code bloom_filter_size} and {@code sorting_columns} are taken from the first primary shard
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetAnalyzeResponse extends BroadcastResponse {

    private final List<ParquetAnalyzeShardResult> shardResults;
    private final boolean shardLevel;

    public ParquetAnalyzeResponse(
        List<ParquetAnalyzeShardResult> shardResults,
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

    public ParquetAnalyzeResponse(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        int size = in.readVInt();
        this.shardResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shardResults.add(new ParquetAnalyzeShardResult(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeVInt(shardResults.size());
        for (ParquetAnalyzeShardResult result : shardResults) {
            result.writeTo(out);
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        if (shardLevel) {
            builder.startArray("shards");
            for (ParquetAnalyzeShardResult result : shardResults) {
                builder.startObject();
                builder.field("shard", result.getShardRouting().shardId().id());
                builder.field("primary", result.getShardRouting().primary());
                inlineShardResultFields(builder, result.getAnalyzeBytes());
                builder.endObject();
            }
            builder.endArray();
        } else {
            if (!shardResults.isEmpty()) {
                List<ParquetAnalyzeShardResult> primaries = shardResults.stream()
                    .filter(r -> r.getShardRouting().primary())
                    .collect(Collectors.toList());
                if (primaries.isEmpty()) {
                    primaries = new ArrayList<>(shardResults);
                }
                if (primaries.size() == 1) {
                    // Single primary: inline directly but strip distinct_count
                    Map<String, Object> map = parseToMap(primaries.get(0).getAnalyzeBytes());
                    stripKey(map, "distinct_count");
                    inlineMap(builder, map);
                } else {
                    inlineAggregatedResults(builder, primaries);
                }
            }
        }
    }

    /**
     * Aggregates multiple shard results by summing numeric fields, then applies post-processing:
     * strips distinct_count and overrides bloom_filter_size/sorting_columns from first primary.
     */
    @SuppressWarnings("unchecked")
    private static void inlineAggregatedResults(XContentBuilder builder, List<ParquetAnalyzeShardResult> results) throws IOException {
        List<Map<String, Object>> parsedResults = new ArrayList<>();
        for (ParquetAnalyzeShardResult result : results) {
            parsedResults.add(parseToMap(result.getAnalyzeBytes()));
        }

        Map<String, Object> merged = new HashMap<>(parsedResults.get(0));
        for (int i = 1; i < parsedResults.size(); i++) {
            deepMergeSum(merged, parsedResults.get(i));
        }

        Map<String, Object> first = parsedResults.get(0);
        // Remove distinct_count (non-additive across shards)
        stripKey(merged, "distinct_count");
        // Override bloom_filter_size and sorting_columns from first primary
        overrideKeyFromFirst(merged, first, "bloom_filter_size");
        overrideKeyFromFirst(merged, first, "sorting_columns");

        inlineMap(builder, merged);
    }

    private static Map<String, Object> parseToMap(BytesReference bytes) {
        if (bytes == null || bytes.length() == 0) return new HashMap<>();
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }

    private static void inlineMap(XContentBuilder builder, Map<String, Object> map) throws IOException {
        XContentBuilder temp = JsonXContent.contentBuilder();
        temp.map(map);
        BytesReference bytes = BytesReference.bytes(temp);
        inlineShardResultFields(builder, bytes);
    }

    @SuppressWarnings("unchecked")
    private static void deepMergeSum(Map<String, Object> base, Map<String, Object> other) {
        for (Map.Entry<String, Object> entry : other.entrySet()) {
            String key = entry.getKey();
            Object otherVal = entry.getValue();
            Object baseVal = base.get(key);
            if (baseVal == null) {
                base.put(key, otherVal);
            } else if (baseVal instanceof Number && otherVal instanceof Number) {
                base.put(key, sumNumbers((Number) baseVal, (Number) otherVal));
            } else if (baseVal instanceof Map && otherVal instanceof Map) {
                deepMergeSum((Map<String, Object>) baseVal, (Map<String, Object>) otherVal);
            }
        }
    }

    private static Number sumNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double || a instanceof Float || b instanceof Float) {
            return a.doubleValue() + b.doubleValue();
        }
        return a.longValue() + b.longValue();
    }

    @SuppressWarnings("unchecked")
    private static void stripKey(Map<String, Object> map, String key) {
        map.remove(key);
        for (Object value : map.values()) {
            if (value instanceof Map) {
                stripKey((Map<String, Object>) value, key);
            } else if (value instanceof List) {
                for (Object item : (List<?>) value) {
                    if (item instanceof Map) {
                        stripKey((Map<String, Object>) item, key);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void overrideKeyFromFirst(Map<String, Object> merged, Map<String, Object> first, String key) {
        if (first.containsKey(key)) {
            merged.put(key, first.get(key));
        }
        for (Map.Entry<String, Object> entry : merged.entrySet()) {
            Object mergedVal = entry.getValue();
            Object firstVal = first.get(entry.getKey());
            if (mergedVal instanceof Map && firstVal instanceof Map) {
                overrideKeyFromFirst((Map<String, Object>) mergedVal, (Map<String, Object>) firstVal, key);
            } else if (mergedVal instanceof List && firstVal instanceof List) {
                List<?> mergedList = (List<?>) mergedVal;
                List<?> firstList = (List<?>) firstVal;
                for (int i = 0; i < mergedList.size() && i < firstList.size(); i++) {
                    if (mergedList.get(i) instanceof Map && firstList.get(i) instanceof Map) {
                        overrideKeyFromFirst((Map<String, Object>) mergedList.get(i), (Map<String, Object>) firstList.get(i), key);
                    }
                }
            }
        }
    }

    private static void inlineShardResultFields(XContentBuilder builder, BytesReference bytes) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                    BytesReference.toBytes(bytes)
                )
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
