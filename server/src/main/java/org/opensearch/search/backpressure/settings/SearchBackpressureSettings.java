/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Settings related to search backpressure and cancellation of in-flight requests.
 *
 * @opensearch.internal
 */
public class SearchBackpressureSettings {
    private static class Defaults {
        private static final long INTERVAL_MILLIS = 1000;
        private static final String MODE = "monitor_only";

        // TODO: decide on default settings for SearchTask
        private static final double CANCELLATION_RATIO_SEARCH_TASK = 0.1;
        private static final double CANCELLATION_RATE_SEARCH_TASK = 0.003;
        private static final double CANCELLATION_BURST_SEARCH_TASK = 10.0;
        private static final double CANCELLATION_RATIO_SEARCH_SHARD_TASK = 0.1;
        private static final double CANCELLATION_RATE_SEARCH_SHARD_TASK = 0.003;
        private static final double CANCELLATION_BURST_SEARCH_SHARD_TASK = 10.0;
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

    /**
     * Defines the percentage of SearchTasks to cancel relative to the number of successful SearchTask completions.
     * In other words, it is the number of tokens added to the bucket on each successful SearchTask completion.
     */
    private volatile double cancellationRatioSearchTask;
    public static final Setting<Double> SETTING_CANCELLATION_RATIO_SEARCH_TASK = Setting.doubleSetting(
        "search_backpressure.cancellation_ratio_search_task",
        Defaults.CANCELLATION_RATIO_SEARCH_TASK,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the number of tasks to cancel per unit time (in millis).
     * In other words, it is the number of tokens added to the bucket each millisecond.
     */
    private volatile double cancellationRateSearchTask;
    public static final Setting<Double> SETTING_CANCELLATION_RATE_SEARCH_TASK = Setting.doubleSetting(
        "search_backpressure.cancellation_rate_search_task",
        Defaults.CANCELLATION_RATE_SEARCH_TASK,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the maximum number of tasks that can be cancelled before being rate-limited.
     */
    private volatile double cancellationBurstSearchTask;
    public static final Setting<Double> SETTING_CANCELLATION_BURST_SEARCH_TASK = Setting.doubleSetting(
        "search_backpressure.cancellation_burst_search_task",
        Defaults.CANCELLATION_BURST_SEARCH_TASK,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the percentage of tasks to cancel relative to the number of successful task completions.
     * In other words, it is the number of tokens added to the bucket on each successful task completion.
     */
    private volatile double cancellationRatioSearchShardTask;
    public static final Setting<Double> SETTING_CANCELLATION_RATIO_SEARCH_SHARD_TASK = Setting.doubleSetting(
        "search_backpressure.cancellation_ratio_search_shard_task",
        Defaults.CANCELLATION_RATIO_SEARCH_SHARD_TASK,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the number of tasks to cancel per unit time (in millis).
     * In other words, it is the number of tokens added to the bucket each millisecond.
     */
    private volatile double cancellationRateSearchShardTask;
    public static final Setting<Double> SETTING_CANCELLATION_RATE_SEARCH_SHARD_TASK = Setting.doubleSetting(
        "search_backpressure.cancellation_rate_search_shard_task",
        Defaults.CANCELLATION_RATE_SEARCH_SHARD_TASK,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the maximum number of tasks that can be cancelled before being rate-limited.
     */
    private volatile double cancellationBurstSearchShardTask;
    public static final Setting<Double> SETTING_CANCELLATION_BURST_SEARCH_SHARD_TASK = Setting.doubleSetting(
        "search_backpressure.cancellation_burst_search_shard_task",
        Defaults.CANCELLATION_BURST_SEARCH_SHARD_TASK,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Callback listeners.
     */
    public interface Listener {
        void onCancellationRatioSearchTaskChanged();

        void onCancellationRateSearchTaskChanged();

        void onCancellationBurstSearchTaskChanged();

        void onCancellationRatioSearchShardTaskChanged();

        void onCancellationRateSearchShardTaskChanged();

        void onCancellationBurstSearchShardTaskChanged();
    }

    private final List<Listener> listeners = new ArrayList<>();
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

        cancellationRatioSearchTask = SETTING_CANCELLATION_RATIO_SEARCH_TASK.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATIO_SEARCH_TASK, this::setCancellationRatioSearchTask);

        cancellationRateSearchTask = SETTING_CANCELLATION_RATE_SEARCH_TASK.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATE_SEARCH_TASK, this::setCancellationRateSearchTask);

        cancellationBurstSearchTask = SETTING_CANCELLATION_BURST_SEARCH_TASK.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_BURST_SEARCH_TASK, this::setCancellationBurstSearchTask);

        cancellationRatioSearchShardTask = SETTING_CANCELLATION_RATIO_SEARCH_SHARD_TASK.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATIO_SEARCH_SHARD_TASK, this::setCancellationRatioSearchShardTask);

        cancellationRateSearchShardTask = SETTING_CANCELLATION_RATE_SEARCH_SHARD_TASK.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_RATE_SEARCH_SHARD_TASK, this::setCancellationRateSearchShardTask);

        cancellationBurstSearchShardTask = SETTING_CANCELLATION_BURST_SEARCH_SHARD_TASK.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CANCELLATION_BURST_SEARCH_SHARD_TASK, this::setCancellationBurstSearchShardTask);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
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

    public double getCancellationRatioSearchTask() {
        return cancellationRatioSearchTask;
    }

    private void setCancellationRatioSearchTask(double cancellationRatioSearchTask) {
        this.cancellationRatioSearchTask = cancellationRatioSearchTask;
        notifyListeners(Listener::onCancellationRatioSearchTaskChanged);
    }

    public double getCancellationRateSearchTask() {
        return cancellationRateSearchTask;
    }

    public double getCancellationRateSearchTaskNanos() {
        return getCancellationRateSearchTask() / TimeUnit.MILLISECONDS.toNanos(1); // rate per nanoseconds
    }

    private void setCancellationRateSearchTask(double cancellationRateSearchTask) {
        this.cancellationRateSearchTask = cancellationRateSearchTask;
        notifyListeners(Listener::onCancellationRateSearchTaskChanged);
    }

    public double getCancellationBurstSearchTask() {
        return cancellationBurstSearchTask;
    }

    private void setCancellationBurstSearchTask(double cancellationBurstSearchTask) {
        this.cancellationBurstSearchTask = cancellationBurstSearchTask;
        notifyListeners(Listener::onCancellationBurstSearchTaskChanged);
    }

    public double getCancellationRatioSearchShardTask() {
        return cancellationRatioSearchShardTask;
    }

    private void setCancellationRatioSearchShardTask(double cancellationRatioSearchShardTask) {
        this.cancellationRatioSearchShardTask = cancellationRatioSearchShardTask;
        notifyListeners(Listener::onCancellationRatioSearchShardTaskChanged);
    }

    public double getCancellationRateSearchShardTask() {
        return cancellationRateSearchShardTask;
    }

    public double getCancellationRateSearchShardTaskNanos() {
        return getCancellationRateSearchShardTask() / TimeUnit.MILLISECONDS.toNanos(1); // rate per nanoseconds
    }

    private void setCancellationRateSearchShardTask(double cancellationRateSearchShardTask) {
        this.cancellationRateSearchShardTask = cancellationRateSearchShardTask;
        notifyListeners(Listener::onCancellationRateSearchShardTaskChanged);
    }

    public double getCancellationBurstSearchShardTask() {
        return cancellationBurstSearchShardTask;
    }

    private void setCancellationBurstSearchShardTask(double cancellationBurstSearchShardTask) {
        this.cancellationBurstSearchShardTask = cancellationBurstSearchShardTask;
        notifyListeners(Listener::onCancellationBurstSearchShardTaskChanged);
    }

    private void notifyListeners(Consumer<Listener> consumer) {
        List<Exception> exceptions = new ArrayList<>();

        for (Listener listener : listeners) {
            try {
                consumer.accept(listener);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
    }
}
