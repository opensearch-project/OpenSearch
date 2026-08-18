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

    private DslQueryExecutorSettings() {}
}
