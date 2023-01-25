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
        permissionIsValidFormat(newPermission);
        return newPermission;
    }

    /**
     * This function creates a permission without checking that the permission string is valid.
     */
    public Permission createLegacyPermission(String permissionString) {
        return new Permission(permissionString);
    }

    /**
     * Check that the permission does not contain any forbidden strings.
     * Assumes that the permission is formatted as resource.action
     */

    public void permissionIsValidFormat(Permission permission) {

        // Check for illegal characters in any of the permission segments O(3n)
        for (String character : INVALID_CHARACTERS) {
            if (permission.permissionType.contains(character) || permission.action.contains(character)) {
                throw new InvalidPermissionException(
                    "The provided permission string for '"
                        + permission.permissionString
                        + "' is not valid. The permission type and action  may not include "
                        + "the character ':' or be empty."
                );
            }
        }

        // Make sure the resource being acted on is one of the qualified permission types
        if (!new ArrayList<String>(List.of(QUALIFIED_PERMISSION_TYPES)).stream().anyMatch(permission.permissionType::equalsIgnoreCase)) {
            throw new InvalidPermissionException(
                "The permission type for '"
                    + permission.permissionString
                    + "' is not valid. Valid permission types are: CLUSTER, INDICES, PLUGIN, and EXTENSION."
            );
        }

        // Require a valid resource pattern for permissions based on indices, plugins, or extensions
        if (permission.permissionType.equalsIgnoreCase("INDICES")
            || permission.permissionType.equalsIgnoreCase("PLUGIN")
            || permission.permissionType.equalsIgnoreCase("EXTENSION")) {
            if (permission.resource.isEmpty()) {
                throw new InvalidPermissionException(
                    "The provided resource pattern for '"
                        + permission.permissionString
                        + "' is not valid. A resource pattern is required for all "
                        + "permissions of types INDICES, PLUGIN, or EXTENSION."
                );

            }
        }
    }

    public static class InvalidPermissionException extends RuntimeException {

        public InvalidPermissionException(String message) {
            super(message);
        }
    }
}
