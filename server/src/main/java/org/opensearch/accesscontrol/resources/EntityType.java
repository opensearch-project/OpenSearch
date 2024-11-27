/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

/**
 * This enum contains the type of entities a resource can be shared with.
 *
 * @opensearch.experimental
 */
public enum EntityType {

    USERS("users"),
    ROLES("roles"),
    BACKEND_ROLES("backend_roles");

    private final String value;

    EntityType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
