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
     * Master switch for Calcite-path routing. Defaults to {@code true} — the plugin routes
     * {@code _search} through the grammar, with codec fallback on grammar rejection or
     * conversion failure. Set to {@code false} to force every request through the codec
     * path unchanged (operational escape hatch: mitigations, benchmarking, incident
     * response).
     *
     * <p>Node-scoped + dynamic → cluster-wide, updatable via {@code PUT _cluster/settings}
     * (use {@code persistent} to survive a full cluster restart).
     */
    public static final Setting<Boolean> CALCITE_ENABLED = Setting.boolSetting(
        "dsl.query_executor.calcite.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private DslQueryExecutorSettings() {}
}
