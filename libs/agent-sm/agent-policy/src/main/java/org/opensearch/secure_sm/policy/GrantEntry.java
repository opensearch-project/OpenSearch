/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.secure_sm.policy;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class GrantEntry {
    public String codeBase;
    private final LinkedList<PermissionEntry> permissionEntries = new LinkedList<>();

    public void add(PermissionEntry entry) {
        permissionEntries.add(entry);
    }

    public List<PermissionEntry> permissionElements() {
        return Collections.unmodifiableList(permissionEntries);
    }
}
