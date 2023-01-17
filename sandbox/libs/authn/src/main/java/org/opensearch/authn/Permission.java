/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

/**
 * A permission of OpenSearch internal resources
 *
 * Example "opensearch.indexing.index.create"
 *
 * @opensearch.experimental
 */
public class Permission {

    private final static String PERMISSION_DELIMITER = "\\.";

    public final static String INVALID_CHARACTERS = ":"; // This is a placeholder for what may want to be banned

    public final String[] permissionChunks;

    public final String permissionString;

    public Permission(final String permission) {
        try {
            this.permissionString = permission;
            this.permissionChunks = permission.split(PERMISSION_DELIMITER);
            if (!permissionIsValidFormat()) {
                throw new Exception();
            }
            ;
        } catch (final Exception e) {
            throw new InvalidPermissionName(permission);
        }
    }

    /**
     * Check that the permission is formatted in the correct syntax
     */
    public boolean permissionIsValidFormat() {

        for (int i = 0; i < this.permissionChunks.length; i++) { // No segments contain invalid characters
            if (this.permissionChunks[i].contains(INVALID_CHARACTERS)) {
                return false;
            }
            if (this.permissionChunks[i].isEmpty()) { // Make sure that no segments are empty i.e. 'opensearch..index.create'
                return false;
            }
        }

        return true;
    }

    public static class InvalidPermissionName extends RuntimeException {
        public InvalidPermissionName(final String name) {
            super("The name '" + name + "' is not a valid permission name");
        }
    }
}
