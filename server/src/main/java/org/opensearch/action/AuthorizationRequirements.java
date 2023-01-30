/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

public enum AuthorizationRequirements {

    CLUSTER_ALLOCATION("cluster_allocation"),
    CLUSTER_CONFIGURATION("cluster_configuration"),
    CLUSTER_DECOMMISSION("cluster_decommission"),
    CLUSTER_HEALTH("cluster_health"),
    CLUSTER_NODE("cluster_node"),
    CLUSTER_REMOTE("cluster_remote"),
    CLUSTER_RESTORE("cluster_restore"),


    SEARCH("search"),
    INDEX("index"),
    PLUGIN("plugin"),
    EXTENSION("extension");

    private final String requestType;


    AuthorizationRequirements(final String requestType) {
        this.requestType = requestType;

    }

    public String getRequestType() {
        return this.requestType;
    }

    public static QUALIFIED_PERMISSION_TYPES matchingType(String instancePermissionType) {
        for (QUALIFIED_PERMISSION_TYPES type : values()) {
            if (type.permissionType.equalsIgnoreCase(instancePermissionType)) {
                return type;
            }
        }
        return null;
    }


}
