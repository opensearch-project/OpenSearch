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
import org.opensearch.search.ResourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * {
 *     "queryGroupID": {
 *          "completions": 1233234234,
 *          "rejections": 1243545,
 *          "failures": 97,
 *          "total_cancellations": 474,
 *          "CPU": { "current_usage": 49.6, "cancellation": 432 },
 *          "MEMORY": { "current_usage": 39.6, "cancellation": 42 }
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
        for (Map.Entry<String, QueryGroupStatsHolder> queryGroupStats : stats.entrySet()) {
            builder.startObject(queryGroupStats.getKey());
            queryGroupStats.getValue().toXContent(builder, params);
            builder.endObject();
        }
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
        private final long completions;
        private final long rejections;
        private final long failures;
        private final long totalCancellations;
        private final Map<ResourceType, ResourceStats> resourceStats;

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
         * Writes the {@param statsHolder} to {@param out}
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
            for (Map.Entry<ResourceType, ResourceStats> resourceStat : resourceStats.entrySet()) {
                ResourceType resourceType = resourceStat.getKey();
                ResourceStats resourceStats1 = resourceStat.getValue();
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
         * Writes the {@param stats} to {@param out}
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
