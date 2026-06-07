/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.common.settings.Setting;

import java.util.List;

/** Cluster-level settings for analytics query execution limits. */
public final class AnalyticsQuerySettings {

    public static final Setting<Integer> MAX_SHARDS_PER_QUERY = Setting.intSetting(
        "analytics.query.max_shards_per_query",
        50,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Max in-flight shard fragment requests <b>per data node</b> for a single query. The coordinator
     * keeps an independent throttle per target node, so total in-flight requests for a query can be
     * up to this value times the number of nodes it fans out to — this bounds the load any single
     * node sees, not the query's overall concurrency.
     */
    public static final Setting<Integer> MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE = Setting.intSetting(
        "analytics.query.max_concurrent_shard_requests_per_node",
        5,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Lane-sizing policy for coordinator-reduce input partitions. See
     * {@link PartitionLanePolicy} for accepted values and semantics.
     */
    public static final Setting<String> REDUCE_PARTITION_LANE_POLICY = Setting.simpleString(
        "analytics.query.reduce.partition_lane_policy",
        PartitionLanePolicy.DEFAULT_VALUE,
        // Eager validation: malformed values throw here, not on every reduce stage.
        PartitionLanePolicy::parse,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> all() {
        return List.of(MAX_SHARDS_PER_QUERY, MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE, REDUCE_PARTITION_LANE_POLICY);
    }

    private AnalyticsQuerySettings() {}
}
