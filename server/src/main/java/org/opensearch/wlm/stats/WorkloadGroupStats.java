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
import org.opensearch.wlm.stats.WorkloadGroupState.ResourceTypeState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {
 *     "workloadGroupID": {
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
public class WorkloadGroupStats implements ToXContentObject, Writeable {
    private final Map<String, WorkloadGroupStatsHolder> stats;

    public WorkloadGroupStats(Map<String, WorkloadGroupStatsHolder> stats) {
        this.stats = stats;
    }

    public WorkloadGroupStats(StreamInput in) throws IOException {
        stats = in.readMap(StreamInput::readString, WorkloadGroupStatsHolder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(stats, StreamOutput::writeString, WorkloadGroupStatsHolder::writeTo);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("workload_groups");
        // to keep the toXContent consistent
        List<Map.Entry<String, WorkloadGroupStatsHolder>> entryList = new ArrayList<>(stats.entrySet());
        entryList.sort((k1, k2) -> k1.getKey().compareTo(k2.getKey()));

        for (Map.Entry<String, WorkloadGroupStatsHolder> workloadGroupStats : entryList) {
            builder.startObject(workloadGroupStats.getKey());
            workloadGroupStats.getValue().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkloadGroupStats that = (WorkloadGroupStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    public Map<String, WorkloadGroupStatsHolder> getStats() {
        return stats;
    }

    /**
     * This is a stats holder object which will hold the data for a workload group at a point in time
     * the instance will only be created on demand through stats api
     */
    public static class WorkloadGroupStatsHolder implements ToXContentObject, Writeable {
        public static final String COMPLETIONS = "total_completions";
        public static final String REJECTIONS = "total_rejections";
        public static final String TOTAL_CANCELLATIONS = "total_cancellations";
        public static final String FAILURES = "failures";
        private long completions;
        private long rejections;
        private long failures;
        private long cancellations;
        private Map<ResourceType, ResourceStats> resourceStats;

        // this is needed to support the factory method
        public WorkloadGroupStatsHolder() {}

        public WorkloadGroupStatsHolder(
            long completions,
            long rejections,
            long failures,
            long cancellations,
            Map<ResourceType, ResourceStats> resourceStats
        ) {
            this.completions = completions;
            this.rejections = rejections;
            this.failures = failures;
            this.cancellations = cancellations;
            this.resourceStats = resourceStats;
        }

        public WorkloadGroupStatsHolder(StreamInput in) throws IOException {
            this.completions = in.readVLong();
            this.rejections = in.readVLong();
            this.failures = in.readVLong();
            this.cancellations = in.readVLong();
            this.resourceStats = in.readMap((i) -> ResourceType.fromName(i.readString()), ResourceStats::new);
        }

        public long getCompletions() {
            return completions;
        }

        public long getRejections() {
            return rejections;
        }

        public long getCancellations() {
            return cancellations;
        }

        public Map<ResourceType, ResourceStats> getResourceStats() {
            return resourceStats;
        }

        /**
         * static factory method to convert {@link WorkloadGroupState} into {@link WorkloadGroupStatsHolder}
         * @param workloadGroupState which needs to be converted
         * @return WorkloadGroupStatsHolder object
         */
        public static WorkloadGroupStatsHolder from(WorkloadGroupState workloadGroupState) {
            final WorkloadGroupStatsHolder statsHolder = new WorkloadGroupStatsHolder();

            Map<ResourceType, ResourceStats> resourceStatsMap = new HashMap<>();

            for (Map.Entry<ResourceType, ResourceTypeState> resourceTypeStateEntry : workloadGroupState.getResourceState().entrySet()) {
                resourceStatsMap.put(resourceTypeStateEntry.getKey(), ResourceStats.from(resourceTypeStateEntry.getValue()));
            }

            statsHolder.completions = workloadGroupState.getTotalCompletions();
            statsHolder.rejections = workloadGroupState.getTotalRejections();
            statsHolder.failures = workloadGroupState.getFailures();
            statsHolder.cancellations = workloadGroupState.getTotalCancellations();
            statsHolder.resourceStats = resourceStatsMap;
            return statsHolder;
        }

        /**
         * Writes the @param {statsHolder} to @param {out}
         * @param out StreamOutput
         * @param statsHolder WorkloadGroupStatsHolder
         * @throws IOException exception
         */
        public static void writeTo(StreamOutput out, WorkloadGroupStatsHolder statsHolder) throws IOException {
            out.writeVLong(statsHolder.completions);
            out.writeVLong(statsHolder.rejections);
            out.writeVLong(statsHolder.failures);
            out.writeVLong(statsHolder.cancellations);
            out.writeMap(statsHolder.resourceStats, (o, val) -> o.writeString(val.getName()), ResourceStats::writeTo);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            WorkloadGroupStatsHolder.writeTo(out, this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(COMPLETIONS, completions);
            // builder.field(SHARD_COMPLETIONS, shardCompletions);
            builder.field(REJECTIONS, rejections);
            // builder.field(FAILURES, failures);
            builder.field(TOTAL_CANCELLATIONS, cancellations);

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
            WorkloadGroupStatsHolder that = (WorkloadGroupStatsHolder) o;
            return completions == that.completions
                && rejections == that.rejections
                && Objects.equals(resourceStats, that.resourceStats)
                && failures == that.failures
                && cancellations == that.cancellations;
        }

        @Override
        public int hashCode() {
            return Objects.hash(completions, rejections, cancellations, failures, resourceStats);
        }
    }

    /**
     * point in time resource level stats holder
     */
    public static class ResourceStats implements ToXContentObject, Writeable {
        public static final String CURRENT_USAGE = "current_usage";
        public static final String CANCELLATIONS = "cancellations";
        public static final String REJECTIONS = "rejections";
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

        public double getCurrentUsage() {
            return currentUsage;
        }

        public long getCancellations() {
            return cancellations;
        }

        public long getRejections() {
            return rejections;
        }

        /**
         * static factory method to convert {@link ResourceTypeState} into {@link ResourceStats}
         * @param resourceTypeState which needs to be converted
         * @return WorkloadGroupStatsHolder object
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
         * @param stats WorkloadGroupStatsHolder
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
            builder.field(REJECTIONS, rejections);
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
