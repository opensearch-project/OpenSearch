/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.common.settings.Setting;

/**
 * Cluster-scoped settings owned by the DSL query executor plugin. Registered via
 * {@link DslQueryExecutorPlugin#getSettings()}.
 */
public final class DslQueryExecutorSettings {

    /**
     * Master switch for Calcite-path routing (default {@code true}). When {@code false}, every
     * {@code _search} is sent through the codec path unchanged.
     *
     * <p>Dynamic and node-scoped, so it can be updated cluster-wide via {@code PUT _cluster/settings}.
     */
    public static final Setting<Boolean> CALCITE_ENABLED = Setting.boolSetting(
        "dsl.query_executor.calcite.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Per-category switch for routing search/hits (and count) requests to Calcite (default
     * {@code true}). When {@code false}, requests that return hits or are non-aggregation
     * searches go through the codec path, even while {@link #CALCITE_AGGREGATION_ENABLED} stays
     * on. Subordinate to {@link #CALCITE_ENABLED}. Dynamic and node-scoped.
     */
    public static final Setting<Boolean> CALCITE_QUERY_ENABLED = Setting.boolSetting(
        "dsl.query_executor.calcite.query.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Per-category switch for routing aggregation requests to Calcite (default {@code true}).
     * When {@code false}, any request that carries aggregations goes through the codec path,
     * even while {@link #CALCITE_QUERY_ENABLED} stays on. Subordinate to {@link #CALCITE_ENABLED}.
     * Dynamic and node-scoped.
     */
    public static final Setting<Boolean> CALCITE_AGGREGATION_ENABLED = Setting.boolSetting(
        "dsl.query_executor.calcite.aggregation.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private DslQueryExecutorSettings() {}
}
