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

/**
 * {
 *     "queryGroupID": {
 *
 *     }
 * }
 */
public class QueryGroupStats implements ToXContentObject, Writeable {
    private final Map<String, QueryGroupStatsHolder> stats;

    public QueryGroupStats(Map<String, QueryGroupStatsHolder> stats) {
        this.stats = stats;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(stats, StreamOutput::writeString, QueryGroupStatsHolder::writeTo);
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    public static class QueryGroupStatsHolder implements ToXContentObject, Writeable {
        private final long completions;
        private final long rejections;
        private final Map<ResourceType, ResourceStats> resourceStats;

        public QueryGroupStatsHolder(long completions, long rejections, Map<ResourceType, ResourceStats> resourceStats) {
            this.completions = completions;
            this.rejections = rejections;
            this.resourceStats = resourceStats;
        }

        public QueryGroupStatsHolder(StreamInput in) throws IOException {
            this.completions = in.readVLong();
            this.rejections = in.readVLong();
            this.resourceStats = in.readMap((i) -> ResourceType.fromName(i.readString()), ResourceStats::new);
        }

        public static void writeTo(StreamOutput out, QueryGroupStatsHolder statsHolder) throws IOException {
            out.writeVLong(statsHolder.completions);
            out.writeVLong(statsHolder.rejections);
            out.writeMap(statsHolder.resourceStats, (o, val) -> o.writeString(val.getName()), ResourceStats::writeTo);
        }


        @Override
        public void writeTo(StreamOutput out) throws IOException {
            QueryGroupStatsHolder.writeTo(out, this);
        }


        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("completions", completions);
            builder.field("rejections", rejections);
            for (Map.Entry<ResourceType, ResourceStats> resourceStat: resourceStats.entrySet()) {
                ResourceType resourceType = resourceStat.getKey();
                ResourceStats resourceStats1 = resourceStat.getValue();
                builder.startObject(resourceType.getName());
                resourceStats1.toXContent(builder, params);
                builder.endObject();
            }
            return builder;
        }
    }

    public static class ResourceStats implements ToXContentObject, Writeable {
        public static final String CURRENT_USAGE = "current_usage";
        public static final String CANCELLATIONS = "cancellations";
        private final double currentUsage;
        private final long cancellations;

        public ResourceStats(double currentUsage, long cancellations) {
            this.currentUsage = currentUsage;
            this.cancellations = cancellations;
        }

        public ResourceStats(StreamInput in) throws IOException {
            this.currentUsage = in.readDouble();
            this.cancellations = in.readVLong();
        }

        public static void writeTo(StreamOutput out, ResourceStats stats) throws IOException {
            out.writeDouble(stats.currentUsage);
            out.writeVLong(stats.cancellations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ResourceStats.writeTo(out, this);
        }


        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(CURRENT_USAGE, currentUsage);
            builder.field(CANCELLATIONS, cancellations);
            return builder;
        }
    }
}
