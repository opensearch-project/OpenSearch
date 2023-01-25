/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to create Permission instances. The factory can create both standard Permissions which have specific
 * formatting requirements and legacy permissions which are not checked for validity on creation.
 */
public class PermissionFactory {

    public final static String[] INVALID_CHARACTERS = new String[] { ":", "" }; // This is a placeholder for what may want to be banned

    // A placeholder for the different resources which a permission may grant a permission based on
    public final static String[] QUALIFIED_PERMISSION_TYPES = new String[] { "cluster", "indices", "plugin", "extension" };

    /**
     * This function creates a standard permission instance. It includes checking that the permission that is being created
     * is properly formatted.
     */
    public Permission createPermission(String permissionString) {

        Permission newPermission = new Permission(permissionString);
        if (permissionIsValidFormat(newPermission)) {
            return newPermission;
        }
        throw new InvalidPermission(permissionString);
    }

    /**
     * This function creates a permission without checking that the permission string is valid.
     */
    public Permission createLegacyPermission(String permissionString) {
        return new Permission(permissionString);
    }

    /**
     * Check that the permission does not contain any forbidden strings.
     * Assumes that the permission is formatted as <resource>.<action>
     */
    public boolean permissionIsValidFormat(Permission permission) {

        // Check for illegal characters in any of the permission segments O(3n)
        for (int i = 0; i < INVALID_CHARACTERS.length; i++) {
            if (permission.resource.contains(INVALID_CHARACTERS[i]) || permission.action.contains(INVALID_CHARACTERS[i])) {
                return false;
            }
        }

        // Make sure the resource being acted on is one of the qualified permission types
        if (!new ArrayList(List.of(QUALIFIED_PERMISSION_TYPES)).contains(permission.permissionType.toUpperCase())) {
            return false;
        }

        // Require a valid resource pattern for permissions based on indices, plugins, or extensions
        if (permission.permissionType.toUpperCase() == "INDICES"
            || permission.permissionType.toUpperCase() == "PLUGIN"
            || permission.permissionType.toUpperCase() == "EXTENSION") {
            if (permission.resource.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public static class InvalidPermission extends RuntimeException {
        public InvalidPermission(final String permission) {
            super("The permission '" + permission + "' is not valid.");
        }
    }
}
