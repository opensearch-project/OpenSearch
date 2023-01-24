/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A basic HashMap implementation of a PermissionStore.
 */
public class PermissionStorage implements PermissionStore {

    public HashMap<String, ArrayList<Permission>> permissionStore = new HashMap<>();

    @Override
    public void put(String principalString, ArrayList<Permission> permissions) {

        permissionStore.put(principalString, permissions);
    }

    @Override
    public ArrayList<Permission> get(String principalString) {

        return permissionStore.get(principalString);
    }

    @Override
    public void delete(String principalString, Permission[] permissions) {

        for (Permission permission : permissions) {
            permissionStore.remove(principalString, permission);
        }
    }

    // Allow for using a String regex expression to delete an entire pair from the map.
    public void delete(String principalString, String regex) {

        if (regex.equals("*")) {
            permissionStore.remove(principalString);
        }
    }
}
