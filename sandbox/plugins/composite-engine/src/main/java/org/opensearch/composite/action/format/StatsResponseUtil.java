/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for format-specific stats response classes.
 *
 * @opensearch.experimental
 */
public final class StatsResponseUtil {

    private StatsResponseUtil() {}

    /** Inlines the fields of a JSON object's bytes into the current builder. */
    public static void inlineJsonBytes(XContentBuilder builder, BytesReference bytes) throws IOException {
        if (bytes == null || bytes.length() == 0) return;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, bytes.streamInput())
        ) {
            if (parser.nextToken() == null) return;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == null) return;
                builder.field(parser.currentName());
                parser.nextToken();
                builder.copyCurrentStructure(parser);
            }
        }
    }

    /**
     * Merges multiple JSON object bytes by summing numeric leaves.
     * For non-numeric or conflicting types, keeps the first value.
     */
    public static BytesReference mergeStatsBytes(List<BytesReference> bytesList) throws IOException {
        if (bytesList.isEmpty()) return new BytesArray("{}");
        if (bytesList.size() == 1) return bytesList.get(0);
        Map<String, Object> merged = parseToMap(bytesList.get(0));
        for (int i = 1; i < bytesList.size(); i++) {
            deepMergeSum(merged, parseToMap(bytesList.get(i)));
        }
        XContentBuilder b = JsonXContent.contentBuilder();
        b.map(merged);
        return BytesReference.bytes(b);
    }

    /** Parses BytesReference JSON into a Map. */
    public static Map<String, Object> parseToMap(BytesReference bytes) {
        if (bytes == null || bytes.length() == 0) return new HashMap<>();
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }

    /**
     * Recursively removes all occurrences of a key from nested maps and lists of maps.
     */
    @SuppressWarnings("unchecked")
    public static void stripKey(Map<String, Object> map, String key) {
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

    /**
     * Recursively overrides all occurrences of a key in merged with the value from first.
     */
    @SuppressWarnings("unchecked")
    public static void overrideKeyFromFirst(Map<String, Object> merged, Map<String, Object> first, String key) {
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
            // For non-numeric, non-map conflicts: keep first value
        }
    }

    private static Number sumNumbers(Number a, Number b) {
        if (a instanceof Double || b instanceof Double || a instanceof Float || b instanceof Float) {
            return a.doubleValue() + b.doubleValue();
        }
        return a.longValue() + b.longValue();
    }
}
