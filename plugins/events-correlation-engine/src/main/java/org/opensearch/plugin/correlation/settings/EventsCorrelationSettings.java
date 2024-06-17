/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;

import static org.opensearch.common.settings.Setting.Property.IndexScope;

/**
 * Settings for events-correlation-engine.
 *
 * @opensearch.api
 * @opensearch.experimental
 */
public class EventsCorrelationSettings {
    /**
     * Correlation Index setting name
     */
    public static final String CORRELATION_INDEX = "index.correlation";
    /**
     * Boolean setting to check if an OS index is a correlation index.
     */
    public static final Setting<Boolean> IS_CORRELATION_INDEX_SETTING = Setting.boolSetting(CORRELATION_INDEX, false, IndexScope);
    public static final Setting<Integer> CORRELATION_HISTORY_INDEX_SHARDS = Setting.intSetting(
        "plugins.events_correlation.correlation_history_index_shards",
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    /**
     * Global time window setting for Correlations
     */
    public static final Setting<TimeValue> CORRELATION_TIME_WINDOW = Setting.positiveTimeSetting(
        "plugins.events_correlation.correlation_time_window",
        new TimeValue(5, TimeUnit.MINUTES),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    /**
     * Global request timeout setting for Correlations
     */
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.positiveTimeSetting(
        "plugins.events_correlation.request_timeout",
        new TimeValue(10, TimeUnit.SECONDS),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default constructor
     */
    public EventsCorrelationSettings() {}
}
