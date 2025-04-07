/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.secure_sm.policy;

import java.util.Objects;

public class PermissionEntry {
    public String permission;
    public String name;
    public String action;

    @Override
    public int hashCode() {
        return Objects.hash(permission, name, action);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;

        return obj instanceof PermissionEntry that
            && Objects.equals(this.permission, that.permission)
            && Objects.equals(this.name, that.name)
            && Objects.equals(this.action, that.action);
    }
}
