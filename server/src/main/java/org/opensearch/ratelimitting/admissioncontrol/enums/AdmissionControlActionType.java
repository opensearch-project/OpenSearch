/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.enums;

import java.util.Locale;

/**
 * Enums that defines the type of the transport requests
 */
public enum AdmissionControlActionType {
    INDEXING("indexing"),
    SEARCH("search"),
    CLUSTER_ADMIN("cluster_admin"),
    FORCE_MERGE("force_merge");

    private final String type;

    AdmissionControlActionType(String uriType) {
        this.type = uriType;
    }

    /**
     *
     * @return type of the request
     */
    public String getType() {
        return type;
    }

    public static AdmissionControlActionType fromName(String name) {
        name = name.toLowerCase(Locale.ROOT);
        switch (name) {
            case "indexing":
                return INDEXING;
            case "search":
                return SEARCH;
            case "cluster_admin":
                return CLUSTER_ADMIN;
            case "force_merge":
                return FORCE_MERGE;
            default:
                throw new IllegalArgumentException("Not Supported TransportAction Type: " + name);
        }
    }
}
