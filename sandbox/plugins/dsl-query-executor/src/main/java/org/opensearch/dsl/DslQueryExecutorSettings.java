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

    /** Master switch for Calcite routing (default true); {@code false} sends every {@code _search} to codec. */
    public static final Setting<Boolean> CALCITE_ENABLED = Setting.boolSetting(
        "dsl.query_executor.calcite.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Routes hits/search (and count) requests to Calcite (default true); {@code false} sends them to codec. */
    public static final Setting<Boolean> CALCITE_QUERY_ENABLED = Setting.boolSetting(
        "dsl.query_executor.calcite.query.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Routes aggregation requests to Calcite (default true); {@code false} sends them to codec. */
    public static final Setting<Boolean> CALCITE_AGGREGATION_ENABLED = Setting.boolSetting(
        "dsl.query_executor.calcite.aggregation.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private DslQueryExecutorSettings() {}
}
