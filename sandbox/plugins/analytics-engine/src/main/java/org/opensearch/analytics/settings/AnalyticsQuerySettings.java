/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.common.settings.Setting;

import java.util.List;

/** Cluster-level settings for analytics query execution limits. */
public final class AnalyticsQuerySettings {

    /** Affix-setting prefix; full key is {@code analytics.delegation.<backend>.blocked_predicates}. */
    public static final String DELEGATION_BLOCKED_PREDICATES_PREFIX = "analytics.delegation.";

    /**
     * Per-backend block-list of predicate functions that must NOT be delegated to that backend. Affix
     * (namespaced) setting: the backend name is the namespace, the value is a list of
     * {@link ScalarFunction} names (case-insensitive). Models the operator-facing
     * {@code Map<BackendName, List<BlockedPredicate>>} contract.
     *
     * <pre>
     * analytics.delegation.lucene.blocked_predicates:  ["LIKE","EQUALS"]
     * </pre>
     *
     * <p>Default empty. Enforced at the marking layer ({@code OpenSearchFilterRule}): a blocked
     * predicate is dropped from that backend's viable set, so the planner leaves it on a non-blocked
     * backend. Dynamic + NodeScope. Registry-derived validation (namespace must be a FILTER-delegation
     * acceptor; predicate must have a serializer on that backend) runs in {@code DelegationBlockList}.
     */
    public static final Setting.AffixSetting<List<ScalarFunction>> DELEGATION_BLOCKED_PREDICATES = Setting.affixKeySetting(
        DELEGATION_BLOCKED_PREDICATES_PREFIX,
        "blocked_predicates",
        key -> Setting.listSetting(key, List.of(), ScalarFunction::fromToken, Setting.Property.NodeScope, Setting.Property.Dynamic)
    );

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

    public static List<Setting<?>> all() {
        return List.of(DELEGATION_BLOCKED_PREDICATES, MAX_SHARDS_PER_QUERY, MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE);
    }

    private AnalyticsQuerySettings() {}
}
