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
 * Centralized configurations for cluster manager tasks.
 * This class maintains task-related constants and configurations, including task keys
 * and their throttling thresholds.
 */
public final class ClusterManagerTaskConfigurations {

    /**
     * Task keys for various cluster manager operations.
     */
    public static final class TaskKeys {
        public static final String CREATE_INDEX_KEY = "create-index";
        public static final String UPDATE_SETTINGS_KEY = "update-settings";
        public static final String CLUSTER_UPDATE_SETTINGS_KEY = "cluster-update-settings";
        public static final String AUTO_CREATE_KEY = "auto-create";
        public static final String DELETE_INDEX_KEY = "delete-index";
        public static final String DELETE_DANGLING_INDEX_KEY = "delete-dangling-index";
        public static final String CREATE_DATA_STREAM_KEY = "create-data-stream";
        public static final String REMOVE_DATA_STREAM_KEY = "remove-data-stream";
        public static final String ROLLOVER_INDEX_KEY = "rollover-index";
        public static final String INDEX_ALIASES_KEY = "index-aliases";
        public static final String PUT_MAPPING_KEY = "put-mapping";
        public static final String CREATE_INDEX_TEMPLATE_KEY = "create-index-template";
        public static final String REMOVE_INDEX_TEMPLATE_KEY = "remove-index-template";
        public static final String CREATE_COMPONENT_TEMPLATE_KEY = "create-component-template";
        public static final String REMOVE_COMPONENT_TEMPLATE_KEY = "remove-component-template";
        public static final String CREATE_INDEX_TEMPLATE_V2_KEY = "create-index-template-v2";
        public static final String REMOVE_INDEX_TEMPLATE_V2_KEY = "remove-index-template-v2";
        public static final String PUT_PIPELINE_KEY = "put-pipeline";
        public static final String DELETE_PIPELINE_KEY = "delete-pipeline";
        public static final String PUT_SEARCH_PIPELINE_KEY = "put-search-pipeline";
        public static final String DELETE_SEARCH_PIPELINE_KEY = "delete-search-pipeline";
        public static final String CREATE_PERSISTENT_TASK_KEY = "create-persistent-task";
        public static final String FINISH_PERSISTENT_TASK_KEY = "finish-persistent-task";
        public static final String REMOVE_PERSISTENT_TASK_KEY = "remove-persistent-task";
        public static final String UPDATE_TASK_STATE_KEY = "update-task-state";
        public static final String CREATE_QUERY_GROUP_KEY = "create-query-group";
        public static final String DELETE_QUERY_GROUP_KEY = "delete-query-group";
        public static final String UPDATE_QUERY_GROUP_KEY = "update-query-group";
        public static final String PUT_SCRIPT_KEY = "put-script";
        public static final String DELETE_SCRIPT_KEY = "delete-script";
        public static final String PUT_REPOSITORY_KEY = "put-repository";
        public static final String DELETE_REPOSITORY_KEY = "delete-repository";
        public static final String CREATE_SNAPSHOT_KEY = "create-snapshot";
        public static final String DELETE_SNAPSHOT_KEY = "delete-snapshot";
        public static final String UPDATE_SNAPSHOT_STATE_KEY = "update-snapshot-state";
        public static final String RESTORE_SNAPSHOT_KEY = "restore-snapshot";
        public static final String CLUSTER_REROUTE_API_KEY = "cluster-reroute-api";

        private TaskKeys() {
            // No instance
        }
    }

    /**
     * Throttling configuration for cluster manager tasks.
     */
    public static final class Throttling {
        private static final int DEFAULT_THROTTLING_THRESHOLD_VALUE = 50;
        private static final ConcurrentMap<String, Integer> TASK_THROTTLING_THRESHOLDS = new ConcurrentHashMap<>();

        static {
            // Tasks with default threshold (50)
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CREATE_INDEX_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.UPDATE_SETTINGS_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CLUSTER_UPDATE_SETTINGS_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.DELETE_INDEX_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.DELETE_DANGLING_INDEX_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CREATE_DATA_STREAM_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.REMOVE_DATA_STREAM_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CREATE_INDEX_TEMPLATE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.REMOVE_INDEX_TEMPLATE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CREATE_COMPONENT_TEMPLATE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.REMOVE_COMPONENT_TEMPLATE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CREATE_INDEX_TEMPLATE_V2_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.REMOVE_INDEX_TEMPLATE_V2_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.PUT_PIPELINE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.DELETE_PIPELINE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.PUT_SEARCH_PIPELINE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.DELETE_SEARCH_PIPELINE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CREATE_PERSISTENT_TASK_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.FINISH_PERSISTENT_TASK_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.REMOVE_PERSISTENT_TASK_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.UPDATE_TASK_STATE_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CREATE_QUERY_GROUP_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.DELETE_QUERY_GROUP_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.UPDATE_QUERY_GROUP_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.PUT_SCRIPT_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.DELETE_SCRIPT_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.PUT_REPOSITORY_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.DELETE_REPOSITORY_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CREATE_SNAPSHOT_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.DELETE_SNAPSHOT_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.RESTORE_SNAPSHOT_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.CLUSTER_REROUTE_API_KEY, DEFAULT_THROTTLING_THRESHOLD_VALUE);

            // Tasks with custom threshold
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.AUTO_CREATE_KEY, 200);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.ROLLOVER_INDEX_KEY, 200);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.INDEX_ALIASES_KEY, 200);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.PUT_MAPPING_KEY, 10000);
            TASK_THROTTLING_THRESHOLDS.put(TaskKeys.UPDATE_SNAPSHOT_STATE_KEY, 5000);
        }

        /**
         * Gets the throttling threshold for a given task key.
         * @param taskKey The key of the cluster manager task.
         * @return The throttling threshold for the task.
         * @throws IllegalArgumentException if the task key is not configured.
         */
        public static int getThreshold(String taskKey) {
            Integer threshold = TASK_THROTTLING_THRESHOLDS.get(taskKey);
            if (threshold == null) {
                throw new IllegalArgumentException(
                    "No throttling threshold configured for task: "
                        + taskKey
                        + ". All cluster manager tasks must have an explicitly configured throttling threshold."
                );
            }
            return threshold;
        }

        private Throttling() {
            // No instance
        }
    }

    private ClusterManagerTaskConfigurations() {
        // No instance
    }
}
