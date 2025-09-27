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

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

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

    public static final Setting<Integer> MAX_THREAD_COUNT_SETTING = new Setting<>(
        "index.merge.scheduler.max_thread_count",
        (s) -> Integer.toString(Math.max(1, Math.min(4, OpenSearchExecutors.allocatedProcessors(s) / 2))),
        (s) -> Setting.parseInt(s, 1, "index.merge.scheduler.max_thread_count"),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Integer> MAX_MERGE_COUNT_SETTING = new Setting<>(
        "index.merge.scheduler.max_merge_count",
        (s) -> Integer.toString(MAX_THREAD_COUNT_SETTING.get(s) + 5),
        (s) -> Setting.parseInt(s, 1, "index.merge.scheduler.max_merge_count"),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Boolean> AUTO_THROTTLE_SETTING = Setting.boolSetting(
        "index.merge.scheduler.auto_throttle",
        true,
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

    private volatile boolean autoThrottle;
    private volatile int maxThreadCount;
    private volatile int maxMergeCount;
    private volatile double maxForceMergeMBPerSec;

    MergeSchedulerConfig(IndexSettings indexSettings) {
        int maxThread = indexSettings.getValue(MAX_THREAD_COUNT_SETTING);
        int maxMerge = indexSettings.getValue(MAX_MERGE_COUNT_SETTING);
        setMaxThreadAndMergeCount(maxThread, maxMerge);
        this.autoThrottle = indexSettings.getValue(AUTO_THROTTLE_SETTING);

        // Index setting takes precedence over cluster setting
        if (MAX_FORCE_MERGE_MB_PER_SEC_SETTING.exists(indexSettings.getSettings())) {
            this.maxForceMergeMBPerSec = indexSettings.getValue(MAX_FORCE_MERGE_MB_PER_SEC_SETTING);
        } else {
            this.maxForceMergeMBPerSec = CLUSTER_MAX_FORCE_MERGE_MB_PER_SEC_SETTING.get(indexSettings.getNodeSettings());
        }
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
    void setAutoThrottle(boolean autoThrottle) {
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
    void setMaxThreadAndMergeCount(int maxThreadCount, int maxMergeCount) {
        if (maxThreadCount < 1) {
            throw new IllegalArgumentException("maxThreadCount should be at least 1");
        }
        if (maxMergeCount < 1) {
            throw new IllegalArgumentException("maxMergeCount should be at least 1");
        }
        if (maxThreadCount > maxMergeCount) {
            throw new IllegalArgumentException(
                "maxThreadCount (= " + maxThreadCount + ") should be <= maxMergeCount (= " + maxMergeCount + ")"
            );
        }
        this.maxThreadCount = maxThreadCount;
        this.maxMergeCount = maxMergeCount;
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
}
