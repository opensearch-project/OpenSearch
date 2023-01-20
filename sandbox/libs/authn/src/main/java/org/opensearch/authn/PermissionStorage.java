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
    public void put(String eventIdentifier, ArrayList<Permission> permissions) {

        permissionStore.put(eventIdentifier, permissions);
    }

    @Override
    public ArrayList<Permission> get(String eventIdentifier) {

        return permissionStore.get(eventIdentifier);
    }

    @Override
    public void delete(String eventIdentifier, Permission[] permissions) {

        for (Permission permission : permissions) {
            permissionStore.remove(eventIdentifier, permission);
        }
    }

    // Allow for using a String regex expression to delete an entire pair from the map.
    public void delete(String eventIdentifier, String regex) {

        if (regex.equals("*")) {
            permissionStore.remove(eventIdentifier);
        }
    }
}
