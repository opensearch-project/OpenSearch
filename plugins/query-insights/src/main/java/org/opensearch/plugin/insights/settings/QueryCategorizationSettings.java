/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.settings;

import org.opensearch.common.settings.Setting;

/**
 * Settings for Query Categorization
 */
public class QueryCategorizationSettings {
    /**
     * Enabling search query metrics
     */
    public static final Setting<Boolean> SEARCH_QUERY_METRICS_ENABLED_SETTING = Setting.boolSetting(
        "search.query.metrics.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default constructor
     */
    public QueryCategorizationSettings() {}

}
