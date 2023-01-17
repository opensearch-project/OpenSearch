/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.opensearch.authn.internal.InternalSubject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class PermissionHandler {

    /**
     * Check that the permission required matches the permission available
     */
    public static boolean matchPermission(final Permission permission, final String permissionRequired) {

        Objects.nonNull(permissionRequired);

        final Permission required = new Permission(permissionRequired); // Could also just pass a permission instead
        for (int i = 0; i < permission.permissionChunks.length; i++) {
            if (!permission.permissionChunks[i].equals(required.permissionChunks[i])) {
                return false;
            }
        }
        return true;
    }

    public static void grantPermission(final Permission permission, final String resource, final InternalSubject subject) {

        // Principal represents the user or plugin which requires the permission on the target resource
        subject.grantedPermissions.putIfAbsent(resource, new ArrayList<Permission>());
        subject.grantedPermissions.get(resource).add(permission);
    }

    public static HashMap<String, ArrayList<Permission>> getPermissions(InternalSubject subject) {

        return subject.grantedPermissions;
    }

    public static void deletePermissions(final Permission permission, final String resource, final InternalSubject subject) {

        subject.grantedPermissions.get(resource).remove(permission);
    }
}
