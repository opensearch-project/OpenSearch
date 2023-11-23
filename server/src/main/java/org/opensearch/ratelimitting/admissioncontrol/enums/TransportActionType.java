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
public enum TransportActionType {
    INDEXING("indexing"),
    SEARCH("search");

    private final String type;

    TransportActionType(String uriType) {
        this.type = uriType;
    }

    /**
     *
     * @return type of the request
     */
    public String getType() {
        return type;
    }

    public static TransportActionType fromName(String name) {
        name = name.toLowerCase(Locale.ROOT);
        switch (name) {
            case "indexing":
                return INDEXING;
            case "search":
                return SEARCH;
            default:
                throw new IllegalArgumentException("Not Supported TransportAction Type: " + name);
        }
    }
}
