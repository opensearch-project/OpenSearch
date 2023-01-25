/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;

/**
 * A basic HashMap implementation of a PermissionStore.
 */
public class PermissionStorage implements PermissionStore {

    public HashMap<Principal, List<Permission>> permissionStore = new HashMap<>();

    @Override
    public void put(Principal principal, List<Permission> permissions) {

        permissionStore.put(principal, permissions);
    }

    @Override
    public List<Permission> get(Principal principal) {

        return permissionStore.get(principal);
    }

    @Override
    public void delete(Principal principal, List<Permission> permissions) {

        for (Permission permission : permissions) {
            permissionStore.remove(principal, permission);
        }
    }
}
