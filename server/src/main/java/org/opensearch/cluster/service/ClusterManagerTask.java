/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.annotation.PublicApi;

/**
 * Task keys and their throttling thresholds for cluster manager operations.
 */
@PublicApi(since = "3.0.0")
public enum ClusterManagerTask {

    // Tasks with default threshold (50)
    CREATE_INDEX("create-index", 50),
    UPDATE_SETTINGS("update-settings", 50),
    CLUSTER_UPDATE_SETTINGS("cluster-update-settings", 50),
    DELETE_INDEX("delete-index", 50),
    DELETE_DANGLING_INDEX("delete-dangling-index", 50),
    CREATE_DATA_STREAM("create-data-stream", 50),
    REMOVE_DATA_STREAM("remove-data-stream", 50),
    CREATE_INDEX_TEMPLATE("create-index-template", 50),
    REMOVE_INDEX_TEMPLATE("remove-index-template", 50),
    CREATE_COMPONENT_TEMPLATE("create-component-template", 50),
    REMOVE_COMPONENT_TEMPLATE("remove-component-template", 50),
    CREATE_INDEX_TEMPLATE_V2("create-index-template-v2", 50),
    REMOVE_INDEX_TEMPLATE_V2("remove-index-template-v2", 50),
    PUT_PIPELINE("put-pipeline", 50),
    DELETE_PIPELINE("delete-pipeline", 50),
    PUT_SEARCH_PIPELINE("put-search-pipeline", 50),
    DELETE_SEARCH_PIPELINE("delete-search-pipeline", 50),
    CREATE_PERSISTENT_TASK("create-persistent-task", 50),
    FINISH_PERSISTENT_TASK("finish-persistent-task", 50),
    REMOVE_PERSISTENT_TASK("remove-persistent-task", 50),
    UPDATE_TASK_STATE("update-task-state", 50),
    CREATE_QUERY_GROUP("create-query-group", 50),
    DELETE_QUERY_GROUP("delete-query-group", 50),
    UPDATE_QUERY_GROUP("update-query-group", 50),
    PUT_SCRIPT("put-script", 50),
    DELETE_SCRIPT("delete-script", 50),
    PUT_REPOSITORY("put-repository", 50),
    DELETE_REPOSITORY("delete-repository", 50),
    CREATE_SNAPSHOT("create-snapshot", 50),
    DELETE_SNAPSHOT("delete-snapshot", 50),
    RESTORE_SNAPSHOT("restore-snapshot", 50),
    CLUSTER_REROUTE_API("cluster-reroute-api", 50),

    // Tasks with custom thresholds
    AUTO_CREATE("auto-create", 200),
    ROLLOVER_INDEX("rollover-index", 200),
    INDEX_ALIASES("index-aliases", 200),
    PUT_MAPPING("put-mapping", 10000),
    UPDATE_SNAPSHOT_STATE("update-snapshot-state", 5000);

    private final String key;
    private final int threshold;

    ClusterManagerTask(String key, int threshold) {
        this.key = key;
        this.threshold = threshold;
    }

    public String getKey() {
        return key;
    }

    public int getThreshold() {
        return threshold;
    }

    public static ClusterManagerTask fromKey(String key) {
        for (ClusterManagerTask task : values()) {
            if (task.getKey().equals(key)) {
                return task;
            }
        }
        throw new IllegalArgumentException("No cluster manager task found for key: " + key);
    }
}
