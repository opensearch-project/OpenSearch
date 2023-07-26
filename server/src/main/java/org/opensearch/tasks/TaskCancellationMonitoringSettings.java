/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Settings related to task cancellation monitoring service.
 */
public class TaskCancellationMonitoringSettings {

    public static final long INTERVAL_MILLIS_SETTING_DEFAULT_VALUE = 5000;
    public static final long DURATION_MILLIS_SETTING_DEFAULT_VALUE = 10000;
    public static final boolean IS_ENABLED_SETTING_DEFAULT_VALUE = true;

    /**
     * Defines the interval(in millis) at which task cancellation service monitors and gather stats.
     */
    public static final Setting<Long> INTERVAL_MILLIS_SETTING = Setting.longSetting(
        "task_cancellation.interval_millis",
        INTERVAL_MILLIS_SETTING_DEFAULT_VALUE,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Setting which defines the duration threshold(in millis) of current running cancelled tasks above which they
     * are tracked as part of stats.
     */
    public static final Setting<Long> DURATION_MILLIS_SETTING = Setting.longSetting(
        "task_cancellation.duration_millis",
        DURATION_MILLIS_SETTING_DEFAULT_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting to enable/disable monitoring service.
     */
    public static final Setting<Boolean> IS_ENABLED_SETTING = Setting.boolSetting(
        "task_cancellation.enabled",
        IS_ENABLED_SETTING_DEFAULT_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final TimeValue interval;
    private TimeValue duration;
    private final AtomicBoolean isEnabled;
    private final Settings settings;
    private final ClusterSettings clusterSettings;

    public TaskCancellationMonitoringSettings(Settings settings, ClusterSettings clusterSettings) {
        this.settings = settings;
        this.clusterSettings = clusterSettings;
        this.interval = new TimeValue(INTERVAL_MILLIS_SETTING.get(settings));
        this.duration = new TimeValue(DURATION_MILLIS_SETTING.get(settings));
        this.isEnabled = new AtomicBoolean(IS_ENABLED_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(IS_ENABLED_SETTING, this::setIsEnabled);
        clusterSettings.addSettingsUpdateConsumer(DURATION_MILLIS_SETTING, this::setDurationMillis);
    }

    public TimeValue getInterval() {
        return this.interval;
    }

    public TimeValue getDuration() {
        return this.duration;
    }

    public void setDurationMillis(long durationMillis) {
        this.duration = new TimeValue(durationMillis);
    }

    public boolean isEnabled() {
        return isEnabled.get();
    }

    public void setIsEnabled(boolean isEnabled) {
        this.isEnabled.set(isEnabled);
    }
}
