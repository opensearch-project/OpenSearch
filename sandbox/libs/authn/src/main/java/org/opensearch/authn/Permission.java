/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

/**
 * An extension of the abstract Permission class which uses String-object Permissions.
 *
 * Example "opensearch.indexing.index.create"
 *
 * @opensearch.experimental
 */
public class Permission extends AbstractPermission {

    private final static String PERMISSION_DELIMITER = "\\.";

    public String permissionString;

    public String[] permissionSegments;

    public String resource;

    public String action;

    public Permission(String permission) {

        this.permissionString = permission;
        this.permissionSegments = permissionString.split(PERMISSION_DELIMITER);
        this.resource = permissionSegments[0];
        this.action = permissionSegments[1];
    }
}
