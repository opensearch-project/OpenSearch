/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.apache.lucene.util.SetOnce;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * Settings related to search backpressure and cancellation of in-flight requests.
 *
 * @opensearch.internal
 */
public class SearchBackpressureSettings {
    private static class Defaults {
        private static final long INTERVAL = 1000;

        private static final boolean ENABLED = true;
        private static final boolean ENFORCED = false;

        private static final double CANCELLATION_RATIO = 0.1;
        private static final double CANCELLATION_RATE = 0.003;
        private static final double CANCELLATION_BURST = 10.0;
    }

    /**
     * Defines the interval (in millis) at which the SearchBackpressureService monitors and cancels tasks.
     */
    private final TimeValue interval;
    public static final Setting<Long> SETTING_INTERVAL = Setting.longSetting(
        "search_backpressure.interval",
        Defaults.INTERVAL,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Defines whether search backpressure is enabled or not.
     */
    private volatile boolean enabled;
    public static final Setting<Boolean> SETTING_ENABLED = Setting.boolSetting(
        "search_backpressure.enabled",
        Defaults.ENABLED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines whether in-flight cancellation of tasks is enabled or not.
     */
    private volatile boolean enforced;
    public static final Setting<Boolean> SETTING_ENFORCED = Setting.boolSetting(
        "search_backpressure.enforced",
        Defaults.ENFORCED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the percentage of tasks to cancel relative to the number of successful task completions.
     * In other words, it is the number of tokens added to the bucket on each successful task completion.
     */
    private volatile double cancellationRatio;
    public static final Setting<Double> SETTING_CANCELLATION_RATIO = Setting.doubleSetting(
        "search_backpressure.cancellation_ratio",
        Defaults.CANCELLATION_RATIO,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the number of tasks to cancel per unit time (in millis).
     * In other words, it is the number of tokens added to the bucket each millisecond.
     */
    private volatile double cancellationRate;
    public static final Setting<Double> SETTING_CANCELLATION_RATE = Setting.doubleSetting(
        "search_backpressure.cancellation_rate",
        Defaults.CANCELLATION_RATE,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the maximum number of tasks that can be cancelled before being rate-limited.
     */
    private volatile double cancellationBurst;
    public static final Setting<Double> SETTING_CANCELLATION_BURST = Setting.doubleSetting(
        "search_backpressure.cancellation_burst",
        Defaults.CANCELLATION_BURST,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Callback listeners.
     */
    public interface Listener {
        void onCancellationRatioChanged();

        void onCancellationRateChanged();

        void onCancellationBurstChanged();
    }

    private final SetOnce<Listener> listener = new SetOnce<>();
    private final NodeDuressSettings nodeDuressSettings;
    private final SearchShardTaskSettings searchShardTaskSettings;

    public SearchBackpressureSettings(Settings settings, ClusterSettings clusterSettings) {
        this.nodeDuressSettings = new NodeDuressSettings(settings, clusterSettings);
        this.searchShardTaskSettings = new SearchShardTaskSettings(settings, clusterSettings);

        interval = new TimeValue(SETTING_INTERVAL.get(settings));

        enabled = SETTING_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ENABLED, this::setEnabled);

        enforced = SETTING_ENFORCED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ENFORCED, this::setEnforced);

        cancellationRatio = SETTING_CANCELLATION_RATIO.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATIO, this::setCancellationRatio);

        cancellationRate = SETTING_CANCELLATION_RATE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATE, this::setCancellationRate);

        cancellationBurst = SETTING_CANCELLATION_BURST.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_BURST, this::setCancellationBurst);
    }

    public void setListener(Listener listener) {
        this.listener.set(listener);
    }

    public NodeDuressSettings getNodeDuressSettings() {
        return nodeDuressSettings;
    }

    public SearchShardTaskSettings getSearchShardTaskSettings() {
        return searchShardTaskSettings;
    }

    public TimeValue getInterval() {
        return interval;
    }

    public boolean isEnabled() {
        return enabled;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnforced() {
        return enforced;
    }

    private void setEnforced(boolean enforced) {
        this.enforced = enforced;
    }

    public double getCancellationRatio() {
        return cancellationRatio;
    }

    private void setCancellationRatio(double cancellationRatio) {
        this.cancellationRatio = cancellationRatio;
        if (listener.get() != null) {
            listener.get().onCancellationRatioChanged();
        }
    }

    public double getCancellationRate() {
        return cancellationRate;
    }

    public double getCancellationRateNanos() {
        return getCancellationRate() / TimeUnit.MILLISECONDS.toNanos(1); // rate per nanoseconds
    }

    private void setCancellationRate(double cancellationRate) {
        this.cancellationRate = cancellationRate;
        if (listener.get() != null) {
            listener.get().onCancellationRateChanged();
        }
    }

    public double getCancellationBurst() {
        return cancellationBurst;
    }

    private void setCancellationBurst(double cancellationBurst) {
        this.cancellationBurst = cancellationBurst;
        if (listener.get() != null) {
            listener.get().onCancellationBurstChanged();
        }
    }
}
