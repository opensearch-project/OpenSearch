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
        key -> Setting.listSetting(
            key,
            key.contains("lucene")
                ? List.of(
                    "IS_NULL",
                    "IS_NOT_NULL",
                    "NOT_EQUALS",
                    "LIKE",
                    "GREATER_THAN",
                    "GREATER_THAN_OR_EQUAL",
                    "LESS_THAN",
                    "LESS_THAN_OR_EQUAL",
                    "SARG_PREDICATE"
                )
                : List.of(),
            ScalarFunction::fromToken,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    );

    /**
     * Fan-out above which a query runs the can-match pre-filter phase before dispatching fragments.
     * Mirrors vanilla's {@code pre_filter_shard_size} ({@code TransportSearchAction
     * .shouldPreFilterSearchShards}), including its default of 128: below it the round trip costs
     * more latency than the handful of shards it could prune is worth.
     *
     * <p>Only consulted when the query has something for the probe to answer — extractable range
     * filters, or a sort to collect bounds for. A sorted query lowers the threshold to 1, matching
     * vanilla's {@code hasPrimaryFieldSort} case: shard ordering and the top-N gate pay for
     * themselves as soon as there is a second shard to order against.
     */
    public static final Setting<Integer> PRE_FILTER_SHARD_SIZE = Setting.intSetting(
        "analytics.query.pre_filter_shard_size",
        128,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * @deprecated inert; superseded by {@code action.search.shard_count.limit}.
     *
     * <p>Registered only so a cluster still carrying this key starts with a deprecation warning
     * rather than failing settings validation. <b>Nothing reads it</b> — the shard ceiling is
     * vanilla's {@code action.search.shard_count.limit}, which
     * {@link org.opensearch.analytics.planner.dag.ShardTargetResolver} reads live.
     *
     * <p>Note the semantics differ, so a value carried over is not equivalent: this one defaulted to
     * 50 and exempted single-index queries; the replacement defaults to unlimited and counts shards
     * regardless of how many indices they span. A deployment that relied on the old ceiling has to
     * set the new key explicitly.
     *
     * <p>Not {@code Dynamic} — a settings-update consumer would imply something acts on changes.
     */
    @Deprecated
    public static final Setting<Integer> MAX_SHARDS_PER_QUERY = Setting.intSetting(
        "analytics.query.max_shards_per_query",
        50,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
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
        return List.of(DELEGATION_BLOCKED_PREDICATES, MAX_SHARDS_PER_QUERY, PRE_FILTER_SHARD_SIZE, MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE);
    }

    private AnalyticsQuerySettings() {}
}
