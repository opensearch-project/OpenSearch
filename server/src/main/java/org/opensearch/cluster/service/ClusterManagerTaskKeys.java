/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

/**
 * Class for maintaining all cluster manager task key at one place.
 */
public final class ClusterManagerTaskKeys {

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
    public static final String PUT_SCRIPT_KEY = "put-script";
    public static final String DELETE_SCRIPT_KEY = "delete-script";
    public static final String PUT_REPOSITORY_KEY = "put-repository";
    public static final String DELETE_REPOSITORY_KEY = "delete-repository";
    public static final String CREATE_SNAPSHOT_KEY = "create-snapshot";
    public static final String DELETE_SNAPSHOT_KEY = "delete-snapshot";
    public static final String UPDATE_SNAPSHOT_STATE_KEY = "update-snapshot-state";
    public static final String RESTORE_SNAPSHOT_KEY = "restore-snapshot";
    public static final String CLUSTER_REROUTE_API_KEY = "cluster-reroute-api";

}
