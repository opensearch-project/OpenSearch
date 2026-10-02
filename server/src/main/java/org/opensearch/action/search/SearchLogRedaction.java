/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Static, node-scoped (non-dynamic) toggle for whether search-related log and task-description
 * sites redact the query source. Defaults to {@code false}, preserving existing behavior; a
 * deployment that wants the redaction sets it in {@code opensearch.yml}.
 *
 * <p>Read once at node bootstrap (mirrors {@link org.opensearch.common.util.FeatureFlags}) —
 * static rather than instance state because the classes that need the value (request DTOs,
 * stateless utilities) have no constructor-level access to {@link Settings}.
 *
 * @opensearch.internal
 */
public final class SearchLogRedaction {

    public static final Setting<Boolean> REDACT_QUERY_LOG_SOURCE = Setting.boolSetting(
        "cluster.search.log.redact_source",
        false,
        Setting.Property.NodeScope
    );

    private static volatile boolean redactSource = false;

    private SearchLogRedaction() {}

    public static void initialize(Settings settings) {
        redactSource = REDACT_QUERY_LOG_SOURCE.get(settings);
    }

    public static boolean shouldRedact() {
        return redactSource;
    }
}
