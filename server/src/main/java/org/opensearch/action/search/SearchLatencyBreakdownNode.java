/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Represents a single node in the hierarchical latency breakdown tree.
 * Each node has a name, category (for color coding), timing information,
 * optional per-shard statistics, and child nodes.
 * <p>
 * This structure enables Gantt-chart style visualization with drill-down:
 * <pre>
 *   Query Rewrite [coordinator] ─────── 14us
 *   ├─ Cluster State Check [coordinator] ── 1us
 *   ├─ Index Resolution [coordinator] ───── 470us
 *   ├─ Shard Selection [coordinator] ────── 607us
 *   └─ Query [phase] ───────────────────── 1.6ms
 *       ├─ Read Lock [search_context] ───── (avg=4us, min=2.5us, max=8.7us, shards=5)
 *       ├─ Acquire Searcher [search_context]
 *       ├─ Query Shard Inf [search_context]
 *       ├─ Search Ctx Creation [search_context]
 *       │   ├─ Query Cache [cache]
 *       │   ├─ Shard Routing [cache]
 *       │   ├─ Queue Wait [concurrency]
 *       │   └─ Queue Wait [concurrency]
 *       ├─ Agg Pre-Process [aggregation]
 *       ├─ Agg Pre-Process [aggregation]
 *       ├─ Agg Post-Process [aggregation]
 *       └─ Agg Post-Process [aggregation]
 * </pre>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class SearchLatencyBreakdownNode implements ToXContentObject, Writeable {

    private final String name;
    private final String category;
    private long durationNanos;

    // Absolute timing for Gantt-chart positioning (relative to request start)
    private long startOffsetNanos;  // offset from request start (0 = beginning)
    private long endOffsetNanos;    // offset from request start

    // Per-shard statistics (for data-node-level operations)
    private int shardCount;
    private long avgNanos;
    private long minNanos;
    private long maxNanos;
    private long totalNanos;

    // Child nodes for hierarchical structure
    private final List<SearchLatencyBreakdownNode> children;

    /**
     * Categories for color coding in the visualization.
     * Maps to the colored tags: coordinator, query, phase, aggregation,
     * pipeline, transport, search_context, cache, concurrency,
     * concurrent_segment_search, fetch, reduce
     */
    public static final String CATEGORY_COORDINATOR = "coordinator";
    public static final String CATEGORY_QUERY = "query";
    public static final String CATEGORY_PHASE = "phase";
    public static final String CATEGORY_AGGREGATION = "aggregation";
    public static final String CATEGORY_PIPELINE = "pipeline";
    public static final String CATEGORY_TRANSPORT = "transport";
    public static final String CATEGORY_SEARCH_CONTEXT = "search_context";
    public static final String CATEGORY_CACHE = "cache";
    public static final String CATEGORY_CONCURRENCY = "concurrency";
    public static final String CATEGORY_CONCURRENT_SEGMENT_SEARCH = "concurrent_segment_search";
    public static final String CATEGORY_FETCH = "fetch";
    public static final String CATEGORY_REDUCE = "reduce";

    /**
     * The set of all valid categories. Used for validation in constructors and deserialization.
     */
    public static final Set<String> VALID_CATEGORIES;

    static {
        Set<String> categories = new HashSet<>();
        categories.add(CATEGORY_COORDINATOR);
        categories.add(CATEGORY_QUERY);
        categories.add(CATEGORY_PHASE);
        categories.add(CATEGORY_AGGREGATION);
        categories.add(CATEGORY_PIPELINE);
        categories.add(CATEGORY_TRANSPORT);
        categories.add(CATEGORY_SEARCH_CONTEXT);
        categories.add(CATEGORY_CACHE);
        categories.add(CATEGORY_CONCURRENCY);
        categories.add(CATEGORY_CONCURRENT_SEGMENT_SEARCH);
        categories.add(CATEGORY_FETCH);
        categories.add(CATEGORY_REDUCE);
        VALID_CATEGORIES = Collections.unmodifiableSet(categories);
    }

    /**
     * Creates a new breakdown node with the given name and category.
     *
     * @param name     the human-readable name of this operation (e.g., "Search Ctx Creation")
     * @param category the category for color-coded visualization; must be one of {@link #VALID_CATEGORIES}
     * @throws IllegalArgumentException if category is not a valid category
     */
    public SearchLatencyBreakdownNode(String name, String category) {
        validateCategory(category);
        this.name = name;
        this.category = category;
        this.children = new ArrayList<>();
    }

    /**
     * Creates a new breakdown node with the given name, category, and duration.
     *
     * @param name          the human-readable name of this operation
     * @param category      the category for color-coded visualization; must be one of {@link #VALID_CATEGORIES}
     * @param durationNanos the duration of the operation in nanoseconds
     * @throws IllegalArgumentException if category is not a valid category
     */
    public SearchLatencyBreakdownNode(String name, String category, long durationNanos) {
        this(name, category);
        this.durationNanos = durationNanos;
    }

    /**
     * Creates a new breakdown node with timing information.
     * The {@code endOffsetNanos} is automatically computed as {@code startOffsetNanos + durationNanos}.
     *
     * @param name             the human-readable name of this operation
     * @param category         the category for color-coded visualization; must be one of {@link #VALID_CATEGORIES}
     * @param startOffsetNanos the start offset relative to request start, in nanoseconds
     * @param durationNanos    the duration of the operation in nanoseconds
     * @throws IllegalArgumentException if category is not a valid category
     */
    public SearchLatencyBreakdownNode(String name, String category, long startOffsetNanos, long durationNanos) {
        this(name, category);
        this.startOffsetNanos = startOffsetNanos;
        this.durationNanos = durationNanos;
        this.endOffsetNanos = startOffsetNanos + durationNanos;
    }

    /**
     * Deserializes a breakdown node from a {@link StreamInput}.
     * Recursively reads all child nodes.
     *
     * @param in the stream input to read from
     * @throws IOException              if an I/O error occurs during deserialization
     * @throws IllegalArgumentException if the deserialized category is not a valid category
     */
    public SearchLatencyBreakdownNode(StreamInput in) throws IOException {
        this.name = in.readString();
        this.category = in.readString();
        validateCategory(this.category);
        this.startOffsetNanos = in.readVLong();
        this.endOffsetNanos = in.readVLong();
        this.durationNanos = in.readVLong();
        this.shardCount = in.readVInt();
        this.avgNanos = in.readVLong();
        this.minNanos = in.readVLong();
        this.maxNanos = in.readVLong();
        this.totalNanos = in.readVLong();
        int childCount = in.readVInt();
        this.children = new ArrayList<>(childCount);
        for (int i = 0; i < childCount; i++) {
            children.add(new SearchLatencyBreakdownNode(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(category);
        out.writeVLong(startOffsetNanos);
        out.writeVLong(endOffsetNanos);
        out.writeVLong(durationNanos);
        out.writeVInt(shardCount);
        out.writeVLong(avgNanos);
        out.writeVLong(minNanos);
        out.writeVLong(maxNanos);
        out.writeVLong(totalNanos);
        out.writeVInt(children.size());
        for (SearchLatencyBreakdownNode child : children) {
            child.writeTo(out);
        }
    }

    // --- Builder methods ---

    public SearchLatencyBreakdownNode setDuration(long nanos) {
        this.durationNanos = nanos;
        return this;
    }

    public SearchLatencyBreakdownNode setStartOffset(long startOffsetNanos) {
        this.startOffsetNanos = startOffsetNanos;
        return this;
    }

    public SearchLatencyBreakdownNode setEndOffset(long endOffsetNanos) {
        this.endOffsetNanos = endOffsetNanos;
        return this;
    }

    public SearchLatencyBreakdownNode setTiming(long startOffsetNanos, long endOffsetNanos) {
        this.startOffsetNanos = startOffsetNanos;
        this.endOffsetNanos = endOffsetNanos;
        this.durationNanos = endOffsetNanos - startOffsetNanos;
        return this;
    }

    public SearchLatencyBreakdownNode setShardStats(int shardCount, long avgNanos, long minNanos, long maxNanos, long totalNanos) {
        this.shardCount = shardCount;
        this.avgNanos = avgNanos;
        this.minNanos = minNanos;
        this.maxNanos = maxNanos;
        this.totalNanos = totalNanos;
        return this;
    }

    public SearchLatencyBreakdownNode addChild(SearchLatencyBreakdownNode child) {
        this.children.add(child);
        return this;
    }

    public SearchLatencyBreakdownNode addChild(String name, String category, long durationNanos) {
        this.children.add(new SearchLatencyBreakdownNode(name, category, durationNanos));
        return this;
    }

    public SearchLatencyBreakdownNode addChild(String name, String category, long startOffsetNanos, long durationNanos) {
        this.children.add(new SearchLatencyBreakdownNode(name, category, startOffsetNanos, durationNanos));
        return this;
    }

    // --- Getters ---

    public String getName() { return name; }
    public String getCategory() { return category; }
    public long getDurationNanos() { return durationNanos; }
    public long getDurationMillis() { return TimeUnit.NANOSECONDS.toMillis(durationNanos); }
    public long getStartOffsetNanos() { return startOffsetNanos; }
    public long getEndOffsetNanos() { return endOffsetNanos; }
    public long getStartOffsetMicros() { return TimeUnit.NANOSECONDS.toMicros(startOffsetNanos); }
    public long getEndOffsetMicros() { return TimeUnit.NANOSECONDS.toMicros(endOffsetNanos); }
    public int getShardCount() { return shardCount; }
    public long getAvgNanos() { return avgNanos; }
    public long getMinNanos() { return minNanos; }
    public long getMaxNanos() { return maxNanos; }
    public long getTotalNanos() { return totalNanos; }
    public List<SearchLatencyBreakdownNode> getChildren() { return children; }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("category", category);

        // Timing: start_offset and duration enable Gantt-chart positioning
        builder.field("start_offset_micros", TimeUnit.NANOSECONDS.toMicros(startOffsetNanos));
        builder.field("duration_micros", TimeUnit.NANOSECONDS.toMicros(durationNanos));
        builder.field("end_offset_micros", TimeUnit.NANOSECONDS.toMicros(endOffsetNanos));

        // Also provide millis for readability
        if (TimeUnit.NANOSECONDS.toMillis(durationNanos) > 0) {
            builder.field("duration_millis", TimeUnit.NANOSECONDS.toMillis(durationNanos));
        }

        // Shard stats (only if multi-shard operation)
        if (shardCount > 0) {
            builder.startObject("shard_stats");
            builder.field("shard_count", shardCount);
            builder.field("avg_micros", TimeUnit.NANOSECONDS.toMicros(avgNanos));
            builder.field("min_micros", TimeUnit.NANOSECONDS.toMicros(minNanos));
            builder.field("max_micros", TimeUnit.NANOSECONDS.toMicros(maxNanos));
            builder.field("total_micros", TimeUnit.NANOSECONDS.toMicros(totalNanos));
            builder.endObject();
        }

        // Children
        if (!children.isEmpty()) {
            builder.startArray("children");
            for (SearchLatencyBreakdownNode child : children) {
                child.toXContent(builder, params);
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    /**
     * Validates that the given category is one of the 12 defined categories.
     *
     * @param category the category to validate
     * @throws IllegalArgumentException if the category is not recognized
     */
    private static void validateCategory(String category) {
        if (category == null || !VALID_CATEGORIES.contains(category)) {
            throw new IllegalArgumentException(
                "Invalid breakdown node category [" + category + "]. Must be one of: " + VALID_CATEGORIES
            );
        }
    }
}
