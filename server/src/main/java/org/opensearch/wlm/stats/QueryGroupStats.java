/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.stats.QueryGroupState.ResourceTypeState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {
 *     "queryGroupID": {
 *          "completions": 1233234234,
 *          "rejections": 12,
 *          "failures": 97,
 *          "total_cancellations": 474,
 *          "CPU": { "current_usage": 49.6, "cancellation": 432, "rejections": 8 },
 *          "MEMORY": { "current_usage": 39.6, "cancellation": 42, "rejections": 4 }
 *     },
 *     ...
 *     ...
 * }
 */
public class QueryGroupStats implements ToXContentObject, Writeable {
    private final Map<String, QueryGroupStatsHolder> stats;

    public QueryGroupStats(Map<String, QueryGroupStatsHolder> stats) {
        this.stats = stats;
    }

    public QueryGroupStats(StreamInput in) throws IOException {
        stats = in.readMap(StreamInput::readString, QueryGroupStatsHolder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(stats, StreamOutput::writeString, QueryGroupStatsHolder::writeTo);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("query_groups");
        // to keep the toXContent consistent
        List<Map.Entry<String, QueryGroupStatsHolder>> entryList = new ArrayList<>(stats.entrySet());
        entryList.sort((k1, k2) -> k1.getKey().compareTo(k2.getKey()));

        for (Map.Entry<String, QueryGroupStatsHolder> queryGroupStats : entryList) {
            builder.startObject(queryGroupStats.getKey());
            queryGroupStats.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryGroupStats that = (QueryGroupStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    /**
     * This is a stats holder object which will hold the data for a query group at a point in time
     * the instance will only be created on demand through stats api
     */
    public static class QueryGroupStatsHolder implements ToXContentObject, Writeable {
        public static final String COMPLETIONS = "completions";
        public static final String REJECTIONS = "rejections";
        public static final String TOTAL_CANCELLATIONS = "total_cancellations";
        public static final String FAILURES = "failures";
        private long completions;
        private long rejections;
        private long failures;
        private long totalCancellations;
        private Map<ResourceType, ResourceStats> resourceStats;

        // this is needed to support the factory method
        public QueryGroupStatsHolder() {}

        public QueryGroupStatsHolder(
            long completions,
            long rejections,
            long failures,
            long totalCancellations,
            Map<ResourceType, ResourceStats> resourceStats
        ) {
            this.completions = completions;
            this.rejections = rejections;
            this.failures = failures;
            this.totalCancellations = totalCancellations;
            this.resourceStats = resourceStats;
        }

        public QueryGroupStatsHolder(StreamInput in) throws IOException {
            this.completions = in.readVLong();
            this.rejections = in.readVLong();
            this.failures = in.readVLong();
            this.totalCancellations = in.readVLong();
            this.resourceStats = in.readMap((i) -> ResourceType.fromName(i.readString()), ResourceStats::new);
        }

        /**
         * static factory method to convert {@link QueryGroupState} into {@link QueryGroupStatsHolder}
         * @param queryGroupState which needs to be converted
         * @return QueryGroupStatsHolder object
         */
        public static QueryGroupStatsHolder from(QueryGroupState queryGroupState) {
            final QueryGroupStatsHolder statsHolder = new QueryGroupStatsHolder();

            Map<ResourceType, ResourceStats> resourceStatsMap = new HashMap<>();

            for (Map.Entry<ResourceType, ResourceTypeState> resourceTypeStateEntry : queryGroupState.getResourceState().entrySet()) {
                resourceStatsMap.put(resourceTypeStateEntry.getKey(), ResourceStats.from(resourceTypeStateEntry.getValue()));
            }

            statsHolder.completions = queryGroupState.getCompletions();
            statsHolder.rejections = queryGroupState.getTotalRejections();
            statsHolder.failures = queryGroupState.getFailures();
            statsHolder.totalCancellations = queryGroupState.getTotalCancellations();
            statsHolder.resourceStats = resourceStatsMap;
            return statsHolder;
        }

        /**
         * Writes the @param {statsHolder} to @param {out}
         * @param out StreamOutput
         * @param statsHolder QueryGroupStatsHolder
         * @throws IOException exception
         */
        public static void writeTo(StreamOutput out, QueryGroupStatsHolder statsHolder) throws IOException {
            out.writeVLong(statsHolder.completions);
            out.writeVLong(statsHolder.rejections);
            out.writeVLong(statsHolder.failures);
            out.writeVLong(statsHolder.totalCancellations);
            out.writeMap(statsHolder.resourceStats, (o, val) -> o.writeString(val.getName()), ResourceStats::writeTo);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            QueryGroupStatsHolder.writeTo(out, this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(COMPLETIONS, completions);
            builder.field(REJECTIONS, rejections);
            builder.field(FAILURES, failures);
            builder.field(TOTAL_CANCELLATIONS, totalCancellations);

            for (ResourceType resourceType : ResourceType.getSortedValues()) {
                ResourceStats resourceStats1 = resourceStats.get(resourceType);
                if (resourceStats1 == null) continue;
                builder.startObject(resourceType.getName());
                resourceStats1.toXContent(builder, params);
                builder.endObject();
            }
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryGroupStatsHolder that = (QueryGroupStatsHolder) o;
            return completions == that.completions
                && rejections == that.rejections
                && Objects.equals(resourceStats, that.resourceStats)
                && failures == that.failures
                && totalCancellations == that.totalCancellations;
        }

        @Override
        public int hashCode() {
            return Objects.hash(completions, rejections, totalCancellations, failures, resourceStats);
        }
    }

    /**
     * point in time resource level stats holder
     */
    public static class ResourceStats implements ToXContentObject, Writeable {
        public static final String CURRENT_USAGE = "current_usage";
        public static final String CANCELLATIONS = "cancellations";
        public static final double PRECISION = 1e-9;
        private final double currentUsage;
        private final long cancellations;
        private final long rejections;

        public ResourceStats(double currentUsage, long cancellations, long rejections) {
            this.currentUsage = currentUsage;
            this.cancellations = cancellations;
            this.rejections = rejections;
        }

        public ResourceStats(StreamInput in) throws IOException {
            this.currentUsage = in.readDouble();
            this.cancellations = in.readVLong();
            this.rejections = in.readVLong();
        }

        /**
         * static factory method to convert {@link ResourceTypeState} into {@link ResourceStats}
         * @param resourceTypeState which needs to be converted
         * @return QueryGroupStatsHolder object
         */
        public static ResourceStats from(ResourceTypeState resourceTypeState) {
            return new ResourceStats(
                resourceTypeState.getLastRecordedUsage(),
                resourceTypeState.cancellations.count(),
                resourceTypeState.rejections.count()
            );
        }

        /**
         * Writes the @param {stats} to @param {out}
         * @param out StreamOutput
         * @param stats QueryGroupStatsHolder
         * @throws IOException exception
         */
        public static void writeTo(StreamOutput out, ResourceStats stats) throws IOException {
            out.writeDouble(stats.currentUsage);
            out.writeVLong(stats.cancellations);
            out.writeVLong(stats.rejections);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ResourceStats.writeTo(out, this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(CURRENT_USAGE, currentUsage);
            builder.field(CANCELLATIONS, cancellations);
            builder.field(QueryGroupStatsHolder.REJECTIONS, rejections);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResourceStats that = (ResourceStats) o;
            return (currentUsage - that.currentUsage) < PRECISION && cancellations == that.cancellations && rejections == that.rejections;
        }

        @Override
        public int hashCode() {
            return Objects.hash(currentUsage, cancellations, rejections);
        }
    }
}
