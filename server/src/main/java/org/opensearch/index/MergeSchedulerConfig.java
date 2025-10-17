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
        DEFAULT_AUTO_THROTTLE,
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
    private static volatile Boolean cachedAutoThrottleEnabledDefault;
    private static volatile Integer cachedMaxThreadCountDefault;
    private static volatile Integer cachedMaxMergeCountDefault;

    MergeSchedulerConfig(IndexSettings indexSettings) {
        indexName = indexSettings.getIndex().getName();
        initMergeConfigs(indexSettings);
        updateMaxForceMergeMBPerSec(indexSettings);
    }

    public synchronized void setDefaultMaxThreadAndMergeCount(int threadCount, int mergeCount, boolean updateActuals) {
        cachedMaxThreadCountDefault = threadCount;
        cachedMaxMergeCountDefault = mergeCount;
        if (updateActuals == true) {
            setMaxThreadAndMergeCount(threadCount, mergeCount);
        }
    }

    public synchronized void setDefaultAutoThrottleEnabled(boolean enabled, boolean updateActuals) {
        cachedAutoThrottleEnabledDefault = enabled;
        if (updateActuals == true) {
            setAutoThrottle(enabled);
        }
    }

    private void initMergeConfigs(IndexSettings indexSettings) {
        Settings settings = indexSettings.getSettings();
        int maxThread = (MAX_THREAD_COUNT_SETTING.exists(settings) == false && cachedMaxThreadCountDefault != null)
            ? cachedMaxThreadCountDefault
            : MAX_THREAD_COUNT_SETTING.get(settings);
        int maxMerge = (MAX_MERGE_COUNT_SETTING.exists(settings) == false && cachedAutoThrottleEnabledDefault != null)
            ? cachedMaxMergeCountDefault
            : MAX_MERGE_COUNT_SETTING.get(settings);
        boolean autoThrottleEnabled = (AUTO_THROTTLE_SETTING.exists(settings) == false && cachedAutoThrottleEnabledDefault != null)
            ? cachedAutoThrottleEnabledDefault
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
    public void setAutoThrottle(boolean autoThrottle) {
        logger.info("setAutoThrottle called with " + autoThrottle);
        logger.info(
            new ParameterizedMessage(
                "Updating autoThrottle for index {} from [{}] to [{}]",
                this.indexName,
                this.autoThrottle,
                autoThrottle
            )
        );
        this.autoThrottle = autoThrottle;
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

    public static String getDefaultMaxThreadCount(Settings settings) {
        if (cachedMaxThreadCountDefault != null) {
            return Integer.toString(cachedMaxThreadCountDefault);
        }
        return Integer.toString(
            Objects.requireNonNullElseGet(
                cachedMaxThreadCountDefault,
                () -> Math.max(1, Math.min(4, OpenSearchExecutors.allocatedProcessors(settings) / 2))
            )
        );

    }

    private static String getDefaultMergeCount(Settings settings) {
        if (cachedMaxMergeCountDefault != null) {
            return Integer.toString(cachedMaxMergeCountDefault);
        }
        return Integer.toString(
            Objects.requireNonNullElseGet(cachedMaxMergeCountDefault, () -> MAX_THREAD_COUNT_SETTING.get(settings) + 5)
        );
    }
}
