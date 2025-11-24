/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.indices.ClusterMergeSchedulerConfig;

import java.util.Objects;

/**
 * The merge scheduler (<code>ConcurrentMergeScheduler</code>) controls the execution of
 * merge operations once they are needed (according to the merge policy).  Merges
 * run in separate threads, and when the maximum number of threads is reached,
 * further merges will wait until a merge thread becomes available.
 *
 * <p>The merge scheduler supports the following <b>dynamic</b> settings:
 *
 * <ul>
 * <li> <code>index.merge.scheduler.max_thread_count</code>:
 * <p>
 *     The maximum number of threads that may be merging at once. Defaults to
 *     <code>Math.max(1, Math.min(4, {@link OpenSearchExecutors#allocatedProcessors(Settings)} / 2))</code>
 *     which works well for a good solid-state-disk (SSD).  If your index is on
 *     spinning platter drives instead, decrease this to 1.
 *
 * <li><code>index.merge.scheduler.auto_throttle</code>:
 * <p>
 *     If this is true (the default), then the merge scheduler will rate-limit IO
 *     (writes) for merges to an adaptive value depending on how many merges are
 *     requested over time.  An application with a low indexing rate that
 *     unluckily suddenly requires a large merge will see that merge aggressively
 *     throttled, while an application doing heavy indexing will see the throttle
 *     move higher to allow merges to keep up with ongoing indexing.
 *
 * <li><code>index.merge.scheduler.max_force_merge_mb_per_sec</code>:
 * <p>
 *     Controls the rate limiting for forced merges in MB per second at the index level.
 *     The default value of Double.POSITIVE_INFINITY means no rate limiting is applied.
 *     Setting a finite positive value will limit the throughput of forced merge operations
 *     to the specified rate. This setting takes precedence over the cluster-level setting.
 *
 * <li><code>cluster.merge.scheduler.max_force_merge_mb_per_sec</code>:
 * <p>
 *     Controls the rate limiting for forced merges in MB per second at the cluster level.
 *     The default value of Double.POSITIVE_INFINITY means no rate limiting is applied.
 *     This setting is used as a fallback when the index-level setting is not specified.
 *     Index-level settings take precedence over this cluster-level setting.
 * </ul>
 *
 * <p><b>Setting Precedence:</b>
 * <ul>
 * <li>If only index-level setting is specified: uses index value
 * <li>If only cluster-level setting is specified: uses cluster value
 * <li>If both settings are specified: uses index value (index takes precedence)
 * <li>If neither setting is specified: uses default value (Double.POSITIVE_INFINITY)
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class MergeSchedulerConfig {
    static Logger logger = LogManager.getLogger(MergeSchedulerConfig.class);
    public static final boolean DEFAULT_AUTO_THROTTLE = true;

    public static final Setting<Integer> MAX_THREAD_COUNT_SETTING = new Setting<>(
        "index.merge.scheduler.max_thread_count",
        MergeSchedulerConfig::getDefaultMaxThreadCount,
        (s) -> Setting.parseInt(s, 1, "index.merge.scheduler.max_thread_count"),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Integer> MAX_MERGE_COUNT_SETTING = new Setting<>(
        "index.merge.scheduler.max_merge_count",
        MergeSchedulerConfig::getDefaultMergeCount,
        (s) -> Setting.parseInt(s, 1, "index.merge.scheduler.max_merge_count"),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Boolean> AUTO_THROTTLE_SETTING = Setting.boolSetting(
        "index.merge.scheduler.auto_throttle",
        MergeSchedulerConfig::getDefaultAutoThrottle,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Double> MAX_FORCE_MERGE_MB_PER_SEC_SETTING = Setting.doubleSetting(
        "index.merge.scheduler.max_force_merge_mb_per_sec",
        Double.POSITIVE_INFINITY,
        0.0d,
        Double.POSITIVE_INFINITY,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Double> CLUSTER_MAX_FORCE_MERGE_MB_PER_SEC_SETTING = Setting.doubleSetting(
        "cluster.merge.scheduler.max_force_merge_mb_per_sec",
        Double.POSITIVE_INFINITY,
        0.0d,
        Double.POSITIVE_INFINITY,
        Property.Dynamic,
        Property.NodeScope
    );

    private final String indexName;
    private volatile boolean autoThrottle;
    private volatile int maxThreadCount;
    private volatile int maxMergeCount;
    private volatile double maxForceMergeMBPerSec;
    private static volatile Boolean clusterAutoThrottleEnabledDefault;
    private static volatile Integer clusterMaxThreadCountDefault;
    private static volatile Integer clusterMaxMergeCountDefault;

    MergeSchedulerConfig(IndexSettings indexSettings) {
        indexName = indexSettings.getIndex().getName();
        initMergeConfigs(indexSettings);
        updateMaxForceMergeMBPerSec(indexSettings);
    }

    /**
     * Sets the default maximum thread and merge count for the cluster.
     * <p>
     * This default value optionally overrides the existing configs for the index
     * if overrideExistingConfigs == true
     * </p>
     *
     * @param threadCount the default maximum number of threads to use for merge operations
     * @param mergeCount the default maximum number of concurrent merges allowed
     * @param overrideExistingConfigs if {@code true}, applies these values to all existing
     *                                configurations in the cluster; if {@code false}, only
     *                                sets the defaults for future configurations
     */
    public synchronized void setDefaultMaxThreadAndMergeCount(int threadCount, int mergeCount, boolean overrideExistingConfigs) {
        clusterMaxThreadCountDefault = threadCount;
        clusterMaxMergeCountDefault = mergeCount;
        if (overrideExistingConfigs == true) {
            setMaxThreadAndMergeCount(threadCount, mergeCount);
        }
    }

    /**
     * Sets the default auto-throttle enabled state for the cluster.
     * <p>
     * This default value optionally overrides the existing configs for the index
     * if overrideExistingConfigs == true
     * </p>
     *
     * @param enabled {@code true} to enable auto-throttling by default,
     *                {@code false} to disable it
     * @param overrideExistingConfigs if {@code true}, applies this setting to all existing
     *                                configurations in the cluster; if {@code false}, only
     *                                sets the default for future configurations
     */
    public synchronized void setDefaultAutoThrottleEnabled(boolean enabled, boolean overrideExistingConfigs) {
        clusterAutoThrottleEnabledDefault = enabled;
        if (overrideExistingConfigs == true) {
            setAutoThrottle(enabled);
        }
    }

    /**
     * Initializes merge scheduler configuration for an index.
     * <p>
     * This method figures out which settings to use (index-level, cluster-level, or absolute defaults)
     * and applies them to this merge scheduler instance.
     * </p>
     *
     * <p><b>Decision Logic:</b>
     * <ul>
     *   <li>If <b>no index-level settings</b> are explicitly set for thread/merge count,
     *       AND cluster defaults exist → use cluster defaults</li>
     *   <li>Otherwise → use the settings as-is (which may include index-level values
     *       or fall back to absolute defaults)</li>
     * </ul>
     *
     * <p><b>What gets initialized:</b></p>
     * <ul>
     *   <li>{@code maxThreadCount} - Maximum number of merge threads</li>
     *   <li>{@code maxMergeCount} - Maximum number of concurrent merges</li>
     *   <li>{@code autoThrottle} - Whether merge I/O throttling is enabled</li>
     * </ul>
     **
     * @param indexSettings the settings for the index being initialized
     */
    private void initMergeConfigs(IndexSettings indexSettings) {
        Settings settings = indexSettings.getSettings();
        boolean useCachedClusterDefaults = MAX_THREAD_COUNT_SETTING.exists(settings) == false
            && MAX_MERGE_COUNT_SETTING.exists(settings) == false
            && clusterMaxThreadCountDefault != null
            && clusterMaxMergeCountDefault != null;

        int maxThread = useCachedClusterDefaults ? clusterMaxThreadCountDefault : MAX_THREAD_COUNT_SETTING.get(settings);

        int maxMerge = useCachedClusterDefaults ? clusterMaxMergeCountDefault : MAX_MERGE_COUNT_SETTING.get(settings);

        boolean autoThrottleEnabled = (AUTO_THROTTLE_SETTING.exists(settings) == false && clusterAutoThrottleEnabledDefault != null)
            ? clusterAutoThrottleEnabledDefault
            : AUTO_THROTTLE_SETTING.get(settings);

        setAutoThrottle(autoThrottleEnabled);
        setMaxThreadAndMergeCount(maxThread, maxMerge);
        logger.info(
            new ParameterizedMessage(
                "Initialized index {} with maxMergeCount={}, maxThreadCount={}, autoThrottleEnabled={}",
                this.indexName,
                this.maxMergeCount,
                this.maxThreadCount,
                this.autoThrottle
            )
        );
    }

    /**
     * Returns <code>true</code> iff auto throttle is enabled.
     *
     * @see ConcurrentMergeScheduler#enableAutoIOThrottle()
     */
    public boolean isAutoThrottle() {
        return autoThrottle;
    }

    /**
     * Enables / disables auto throttling on the {@link ConcurrentMergeScheduler}
     */
    public void setAutoThrottle(boolean enabled) {
        logger.info(
            new ParameterizedMessage("Updating autoThrottle for index {} from [{}] to [{}]", this.indexName, this.autoThrottle, enabled)
        );
        this.autoThrottle = enabled;
    }

    /**
     * Returns {@code maxThreadCount}.
     */
    public int getMaxThreadCount() {
        return maxThreadCount;
    }

    /**
     * Expert: directly set the maximum number of merge threads and
     * simultaneous merges allowed.
     */
    public void setMaxThreadAndMergeCount(int newMaxThreadCount, int newMaxMergeCount) {
        if (newMaxThreadCount == this.maxThreadCount && newMaxMergeCount == this.maxMergeCount) {
            return;
        }
        validateMaxThreadAndMergeCount(newMaxThreadCount, newMaxMergeCount);
        logger.info(
            new ParameterizedMessage(
                "Updating maxThreadCount from [{}] to [{}] and maxMergeCount from [{}] to [{}] for index {}.",
                this.maxThreadCount,
                newMaxThreadCount,
                this.maxMergeCount,
                newMaxMergeCount,
                this.indexName
            )
        );
        this.maxThreadCount = newMaxThreadCount;
        this.maxMergeCount = newMaxMergeCount;
    }

    /**
     * Validate the values of {@code maxMergeCount} and {@code maxThreadCount}
     */
    public static void validateMaxThreadAndMergeCount(int maxThreadCount, int maxMergeCount) {
        if (maxThreadCount < 1) {
            throw new IllegalArgumentException("maxThreadCount (= " + maxThreadCount + ") should be at least 1");
        }
        if (maxMergeCount < 1) {
            throw new IllegalArgumentException("maxMergeCount (= " + maxMergeCount + ") should be at least 1");
        }
        if (maxThreadCount > maxMergeCount) {
            throw new IllegalArgumentException(
                "maxThreadCount (= " + maxThreadCount + ") should be <= maxMergeCount (= " + maxMergeCount + ")"
            );
        }
    }

    /**
     * Returns {@code maxMergeCount}.
     */
    public int getMaxMergeCount() {
        return maxMergeCount;
    }

    /**
     * Returns the maximum force merge rate in MB per second.
     * A value of Double.POSITIVE_INFINITY indicates no rate limiting.
     */
    public double getMaxForceMergeMBPerSec() {
        return maxForceMergeMBPerSec;
    }

    /**
     * Sets the maximum force merge rate in MB per second.
     * A value of Double.POSITIVE_INFINITY disables rate limiting.
     */
    void setMaxForceMergeMBPerSec(double maxForceMergeMBPerSec) {
        this.maxForceMergeMBPerSec = maxForceMergeMBPerSec;
    }

    /**
     * Updates the maximum force merge rate based on index settings, with fallback to cluster settings.
     * This method handles the case where an index-level setting is removed and should
     * fall back to the cluster-level setting.
     */
    public void updateMaxForceMergeMBPerSec(IndexSettings indexSettings) {
        boolean hasIndexSetting = MAX_FORCE_MERGE_MB_PER_SEC_SETTING.exists(indexSettings.getSettings());
        if (hasIndexSetting) {
            this.maxForceMergeMBPerSec = indexSettings.getValue(MAX_FORCE_MERGE_MB_PER_SEC_SETTING);
        } else {
            this.maxForceMergeMBPerSec = CLUSTER_MAX_FORCE_MERGE_MB_PER_SEC_SETTING.get(indexSettings.getNodeSettings());
        }
    }

    private static String getDefaultMaxThreadCount(Settings settings) {
        // If MAX_MERGE_COUNT_SETTING is set - return the absolute default for MAX_THREAD_COUNT_SETTING
        // If MAX_MERGE_COUNT_SETTING is NOT set - return the cluster default
        // On first invocation, MAX_MERGE_COUNT_SETTING would not have been initialized, hence the null check
        if (MAX_MERGE_COUNT_SETTING == null || MAX_MERGE_COUNT_SETTING.exists(settings) == true || clusterMaxThreadCountDefault == null) {
            return ClusterMergeSchedulerConfig.getClusterMaxThreadCountDefault(settings);
        }
        return Integer.toString(clusterMaxThreadCountDefault);
    }

    private static String getDefaultMergeCount(Settings settings) {
        // If MAX_THREAD_COUNT_SETTING is set - return the absolute default for MAX_MERGE_COUNT_SETTING
        // If MAX_THREAD_COUNT_SETTING is NOT set - return the cluster default
        if (MAX_THREAD_COUNT_SETTING.exists(settings) == true || clusterMaxMergeCountDefault == null) {
            return Integer.toString(MAX_THREAD_COUNT_SETTING.get(settings) + 5);
        }
        return Integer.toString(clusterMaxMergeCountDefault);
    }

    private static String getDefaultAutoThrottle(Settings settings) {
        return Boolean.toString(Objects.requireNonNullElseGet(clusterAutoThrottleEnabledDefault, () -> DEFAULT_AUTO_THROTTLE));
    }
}
