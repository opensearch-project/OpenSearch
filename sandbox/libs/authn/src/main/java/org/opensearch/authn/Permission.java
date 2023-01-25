/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

/**
 * A class that defines a Permission.
 *
 * Example "opensearch.indexing.index.create"
 *
 * @opensearch.experimental
 */
public class Permission {

    public final String PERMISSION_DELIMITER = "\\.";

    public final String ACTION_DELIMITER = "/";

    public String permissionString;

    public String[] permissionSegments;

    public String resource;

    public String action;
    public String permissionType;

    public Permission(String permission) {

        this.permissionString = permission;
        this.permissionSegments = permissionString.split(PERMISSION_DELIMITER);
        this.permissionType = permissionSegments[0];
        this.action = permissionSegments[1];
        if (this.permissionSegments.length == 3) {
            this.resource = permissionSegments[2];
        }
    }
}
