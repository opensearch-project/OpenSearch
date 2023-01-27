/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

/**
 * A class that defines a Permission.
 *
 * Example "cluster.write"
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
        try {
            this.permissionSegments = permissionString.split(PERMISSION_DELIMITER);
            this.permissionType = permissionSegments[0];
            this.action = permissionSegments[1];
        } catch (IndexOutOfBoundsException ex) {
            throw new PermissionFactory.InvalidPermissionException(
                "All permissions must contain a permission type and" + " action delimited by a \".\"."
            );
        }
        if (this.permissionSegments.length == 3) {
            this.resource = permissionSegments[2];
        }
    }
}
