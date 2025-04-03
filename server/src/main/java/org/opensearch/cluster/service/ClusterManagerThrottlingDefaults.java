/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Class for maintaining default throttling thresholds across all cluster manager tasks at one place.
 */
public final class ClusterManagerThrottlingDefaults {
    private static final int DEFAULT_THRESHOLD_VALUE = 50;
    private static final ConcurrentMap<String, Integer> DEFAULT_THRESHOLDS = new ConcurrentHashMap<>();

    static {
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CREATE_INDEX_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.UPDATE_SETTINGS_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CLUSTER_UPDATE_SETTINGS_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.AUTO_CREATE_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.DELETE_INDEX_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.DELETE_DANGLING_INDEX_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CREATE_DATA_STREAM_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.REMOVE_DATA_STREAM_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.ROLLOVER_INDEX_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.INDEX_ALIASES_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.PUT_MAPPING_KEY, 10000);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CREATE_INDEX_TEMPLATE_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.REMOVE_INDEX_TEMPLATE_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CREATE_COMPONENT_TEMPLATE_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.REMOVE_COMPONENT_TEMPLATE_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CREATE_INDEX_TEMPLATE_V2_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.REMOVE_INDEX_TEMPLATE_V2_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.PUT_PIPELINE_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.DELETE_PIPELINE_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.PUT_SEARCH_PIPELINE_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.DELETE_SEARCH_PIPELINE_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CREATE_PERSISTENT_TASK_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.FINISH_PERSISTENT_TASK_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.REMOVE_PERSISTENT_TASK_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.UPDATE_TASK_STATE_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CREATE_QUERY_GROUP_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.DELETE_QUERY_GROUP_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.UPDATE_QUERY_GROUP_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.PUT_SCRIPT_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.DELETE_SCRIPT_KEY, 200);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.PUT_REPOSITORY_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.DELETE_REPOSITORY_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CREATE_SNAPSHOT_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.DELETE_SNAPSHOT_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.UPDATE_SNAPSHOT_STATE_KEY, 5000);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.RESTORE_SNAPSHOT_KEY, 50);
        DEFAULT_THRESHOLDS.put(ClusterManagerTaskKeys.CLUSTER_REROUTE_API_KEY, 50);
    }

    public static int getDefaultThreshold(String taskKey) {
        return DEFAULT_THRESHOLDS.getOrDefault(taskKey, DEFAULT_THRESHOLD_VALUE);
    }
}
