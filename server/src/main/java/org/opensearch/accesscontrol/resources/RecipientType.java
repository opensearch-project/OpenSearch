/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

/**
 * This class determines a type of recipient a resource can be shared with.
 * An example type would be a user or a role.
 * This class is used to determine the type of recipient a resource can be shared with.
 * @opensearch.experimental
 */
public class RecipientType {
    private final String type;

    public RecipientType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return type;
    }
}
