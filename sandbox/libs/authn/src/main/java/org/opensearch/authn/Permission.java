/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import java.io.IOException;
import java.util.Objects;

public class Permission {
    private final static String PERMISSION_DELIMITER = "\\.";  
    private final String[] permissionChunks; 

    public Permission(final String permission) {
        try {
            this.permissionChunks = permission.split(PERMISSION_DELIMITER);
        } catch (Exception) {
            throw new InvalidPermissionName(permission);
        }
    }

    public boolean matches(final String permissionRequired) {
        Objects.nonNull(permissionRequired);

        final Permission required = new Permission(permissionRequired);
        for(int i = 0; i < this.permissionChunks.length; i++) {
            if (!this.permissionChunks[i].equals(required.permissionChunks[i])) {
                return false;
            }
        }
        return true;
    }

    public static void checkIsValid(final String permission) {
        new Permission(permission);
    }

    public static class InvalidPermissionName extends RuntimeException {
        public InvalidPermissionName(final String name) {
            super("The name '" + name + "' is not a valid permission name");
        }
    }
}
