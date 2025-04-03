/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.security.CodeSource;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

public class PolicyEntry {

    private final CodeSource codesource;
    final List<Permission> permissions;

    PolicyEntry(CodeSource cs) {
        this.codesource = cs;
        this.permissions = new ArrayList<Permission>();
    }

    /**
     * add a Permission object to this entry.
     * No need to sync add op because perms are added to entry only
     * while entry is being initialized
     */
    void add(Permission p) {
        permissions.add(p);
    }

    CodeSource getCodeSource() {
        return codesource;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append(getCodeSource()).append("\n");
        for (Permission p : permissions) {
            sb.append("  ").append(p).append("\n");
        }
        sb.append("}\n");
        return sb.toString();
    }
}
