/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

public enum AdminRequirements implements AuthorizationRequirements {

    CLUSTER_ALLOCATION("cluster_allocation", new String[] {"index", "shard", "primary"}),
    CLUSTER_CONFIGURATION("cluster_configuration", new String[] {}),
    CLUSTER_DECOMMISSION("cluster_decommission", new String[] {"attribute"}),
    CLUSTER_HEALTH("cluster_health", new String[] {}),
    CLUSTER_NODE("cluster_node", new String[] {}),
    CLUSTER_REMOTE("cluster_remote", new String[] {}),
    CLUSTER_RESTORE("cluster_restore"),
    CLUSTER_REPOSITORY("cluster_repository"),
    CLUSTER_REROUTE("cluster_reroute"),
    CLUSTER_SETTINGS("cluster_settings"),
    CLUSTER_SHARDS("cluster_shards"),
    CLUSTER_SNAPSHOTS("cluster_snapshots"),
    CLUSTER_STATE("cluster_state"),
    CLUSTER_STATS("cluster_stats"),
    CLUSTER_SCRIPTS("cluster_scripts"),
    CLUSTER_TASKS("cluster_tasks");

    private final String requestType;

    private final String[] requestParameters;

    AdminRequirements(final String requestType, final String[] requestParameters) {
        this.requestType = requestType;
        this.requestParameters = requestParameters;
    }

    public String getRequestType() {
        return this.requestType;
    }
}
