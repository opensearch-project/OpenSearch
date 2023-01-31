/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

/**
 * This class is used to create Permission instances. The factory can create both standard Permissions which have specific
 * formatting requirements and legacy permissions which are not checked for validity on creation.
 */
public class PermissionFactory {

    private final static String[] INVALID_CHARACTERS = new String[] { ":", "" }; // This is a placeholder for what may want to be banned

    // A placeholder for the different resources which a permission may grant a permission based on
    public enum QUALIFIED_PERMISSION_TYPES {

        CLUSTER("cluster", false),
        INDICES("indices", true),
        PLUGIN("plugin", true),
        EXTENSION("extension", true);

        private final String permissionType;
        private final boolean patternRequired;

        QUALIFIED_PERMISSION_TYPES(final String permissionType, boolean patternRequired) {
            this.permissionType = permissionType;
            this.patternRequired = patternRequired;
        }

        public String getPermissionType() {
            return this.permissionType;
        }

        public boolean isResourcePatternRequired() {
            return this.patternRequired;
        }

        public static QUALIFIED_PERMISSION_TYPES matchingType(String instancePermissionType) {
            for (QUALIFIED_PERMISSION_TYPES type : values()) {
                if (type.permissionType.equalsIgnoreCase(instancePermissionType)) {
                    return type;
                }
            }
            return null;
        }
    }

    /**
     * This function creates a standard permission instance. It includes checking that the permission that is being created
     * is properly formatted.
     */
    public OpenSearchPermission createPermission(String permissionString) {

        OpenSearchPermission newPermission = new OpenSearchPermission(permissionString);

        permissionIsValidFormat(newPermission);
        return newPermission;
    }

    /**
     * This function creates a permission without checking that the permission string is valid.
     */
    public OpenSearchPermission createLegacyPermission(String permissionString) {
        return new OpenSearchPermission(permissionString);

    }

    /**
     * Check that the permission does not contain any invalid characters
     */

    public void checkForInvalidCharacters(OpenSearchPermission permission) {
        for (String invalidCharacter : INVALID_CHARACTERS) {
            if (permission.getPermissionType().contains(invalidCharacter) || permission.getAction().contains(invalidCharacter)) {
                throw new InvalidPermissionException(
                    "The provided permission string for '"
                        + permission.getPermissionString()
                        + "' is not valid. The permission type and action may not include "
                        + "the character "
                        + INVALID_CHARACTERS.toString()
                        + " or be empty."
                );
            }
        }
    }

    /**
     * Make sure the permission type is one of the qualified permission types
     */

    public void checkForValidPermissionType(OpenSearchPermission permission) {
        if (QUALIFIED_PERMISSION_TYPES.matchingType(permission.getPermissionType()) == null) {
            throw new InvalidPermissionException(
                "The permission type for '"
                    + permission.getPermissionString()
                    + "' is not valid. Valid permission types are: "
                    + QUALIFIED_PERMISSION_TYPES.values()
            );
        }
    }

    /**
     * Make sure a resource pattern is present for permission types that require one
     */
    public void checkIfResourcePatternIsRequiredAndPresent(OpenSearchPermission permission) {
        QUALIFIED_PERMISSION_TYPES permissionType = QUALIFIED_PERMISSION_TYPES.matchingType(permission.getPermissionType());
        assert permissionType != null;
        if (permissionType.patternRequired && (permission.getResource() == null || permission.getResource().isEmpty())) {
            throw new InvalidPermissionException(
                "The provided resource pattern for '"
                    + permission.getPermissionString()
                    + "' is not valid. A resource pattern is required for all "
                    + "permissions of type "
                    + permissionType
            );
        }
    }

    /**
     * Check that the permission does not contain any forbidden strings.
     * Assumes that the permission is formatted as resource.action
     */
    public void permissionIsValidFormat(OpenSearchPermission permission) {

        // Make sure no invalid characters are present O(3n)
        checkForInvalidCharacters(permission);

        // Make sure the resource being acted on is one of the qualified permission types
        checkForValidPermissionType(permission);

        // Require a valid resource pattern for permissions based on indices, plugins, or extensions
        checkIfResourcePatternIsRequiredAndPresent(permission);
    }

    public static class InvalidPermissionException extends RuntimeException {

        public InvalidPermissionException(String message) {
            super(message);
        }
    }
}
