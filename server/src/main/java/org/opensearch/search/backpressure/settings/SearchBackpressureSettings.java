/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Settings related to search backpressure mode and interval
 *
 * @opensearch.internal
 */
public class SearchBackpressureSettings {
    private static class Defaults {
        private static final long INTERVAL_MILLIS = 1000;
        private static final String MODE = "monitor_only";

        private static final double CANCELLATION_RATIO = 0.1;
        private static final double CANCELLATION_RATE = 0.003;
        private static final double CANCELLATION_BURST = 10.0;
    }

    /**
     * Defines the interval (in millis) at which the SearchBackpressureService monitors and cancels tasks.
     */
    private final TimeValue interval;
    public static final Setting<Long> SETTING_INTERVAL_MILLIS = Setting.longSetting(
        "search_backpressure.interval_millis",
        Defaults.INTERVAL_MILLIS,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Defines the search backpressure mode. It can be either "disabled", "monitor_only" or "enforced".
     */
    private volatile SearchBackpressureMode mode;
    public static final Setting<SearchBackpressureMode> SETTING_MODE = new Setting<>(
        "search_backpressure.mode",
        Defaults.MODE,
        SearchBackpressureMode::fromName,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the percentage of tasks to cancel relative to the number of successful task completions.
     * In other words, it is the number of tokens added to the bucket on each successful task completion.
     *
     * The setting below is deprecated.
     * To keep backwards compatibility, the old usage is remained, and it's also used as the fallback for the new usage.
     */
    public static final Setting<Double> SETTING_CANCELLATION_RATIO = Setting.doubleSetting(
        "search_backpressure.cancellation_ratio",
        Defaults.CANCELLATION_RATIO,
        0.0,
        1.0,
        Setting.Property.Deprecated,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the number of tasks to cancel per unit time (in millis).
     * In other words, it is the number of tokens added to the bucket each millisecond.
     *
     * The setting below is deprecated.
     * To keep backwards compatibility, the old usage is remained, and it's also used as the fallback for the new usage.
     */
    public static final Setting<Double> SETTING_CANCELLATION_RATE = Setting.doubleSetting(
        "search_backpressure.cancellation_rate",
        Defaults.CANCELLATION_RATE,
        0.0,
        Setting.Property.Deprecated,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the maximum number of tasks that can be cancelled before being rate-limited.
     *
     * The setting below is deprecated.
     * To keep backwards compatibility, the old usage is remained, and it's also used as the fallback for the new usage.
     */
    public static final Setting<Double> SETTING_CANCELLATION_BURST = Setting.doubleSetting(
        "search_backpressure.cancellation_burst",
        Defaults.CANCELLATION_BURST,
        1.0,
        Setting.Property.Deprecated,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final Settings settings;
    private final ClusterSettings clusterSettings;
    private final NodeDuressSettings nodeDuressSettings;
    private final SearchTaskSettings searchTaskSettings;
    private final SearchShardTaskSettings searchShardTaskSettings;

    public SearchBackpressureSettings(Settings settings, ClusterSettings clusterSettings) {
        this.settings = settings;
        this.clusterSettings = clusterSettings;
        this.nodeDuressSettings = new NodeDuressSettings(settings, clusterSettings);
        this.searchTaskSettings = new SearchTaskSettings(settings, clusterSettings);
        this.searchShardTaskSettings = new SearchShardTaskSettings(settings, clusterSettings);

        interval = new TimeValue(SETTING_INTERVAL_MILLIS.get(settings));

        mode = SETTING_MODE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_MODE, this::setMode);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATIO, searchShardTaskSettings::setCancellationRatio);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATE, searchShardTaskSettings::setCancellationRate);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_BURST, searchShardTaskSettings::setCancellationBurst);
    }

    public Settings getSettings() {
        return settings;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public NodeDuressSettings getNodeDuressSettings() {
        return nodeDuressSettings;
    }

    public SearchTaskSettings getSearchTaskSettings() {
        return searchTaskSettings;
    }

    public SearchShardTaskSettings getSearchShardTaskSettings() {
        return searchShardTaskSettings;
    }

    public TimeValue getInterval() {
        return interval;
    }

    public SearchBackpressureMode getMode() {
        return mode;
    }

    public void setMode(SearchBackpressureMode mode) {
        this.mode = mode;
    }
}
