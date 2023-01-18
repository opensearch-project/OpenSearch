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
 * Settings related to search backpressure mode and internal
 *
 * @opensearch.internal
 */
public class SearchBackpressureSettings {
    private static class Defaults {
        private static final long INTERVAL_MILLIS = 1000;
        private static final String MODE = "monitor_only";
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
    public static final Setting<String> SETTING_MODE = Setting.simpleString(
        "search_backpressure.mode",
        Defaults.MODE,
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

        mode = SearchBackpressureMode.fromName(SETTING_MODE.get(settings));
        clusterSettings.addSettingsUpdateConsumer(SETTING_MODE, s -> this.setMode(SearchBackpressureMode.fromName(s)));
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
