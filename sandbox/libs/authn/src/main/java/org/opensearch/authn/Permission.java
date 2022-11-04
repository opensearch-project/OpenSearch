/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import java.io.IOException;
import java.util.Objects;

public class Permission {
    private final String[] permissionChunks; 

    public Permission(final String permission) {
        this.permissionChunks = permission.split("\\.");
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
}
