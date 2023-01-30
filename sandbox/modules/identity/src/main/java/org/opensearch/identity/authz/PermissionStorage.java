/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;

/**
 * * This class represents a basic permission store. A permission store should be a data structure which
 * supports add, get, and delete operations and can store "String : ListofPermission" pairs or simulate this storage.
 * The underlying data structure can be options other than a HashMap as long as the implementation
 * has a way to translate between the required functions and the data structure below.
 *
 * For example, an Array-based solution could be implemented by appending the String to the front of every Permission in
 * the List and then storing the Permissions in a Permission[]. You would also need to implement the add, get, and delete
 * methods to support this structure effectively.
 *
 * @opensearch.experimental
 */
public class PermissionStorage {

    public HashMap<Principal, List<Permission>> permissionStore = new HashMap<>();

    /**
     * This function adds a set of permissions to the permission store. The principal is a unique identifier for some subject.
     * The List is a list of all permissions that are being granted.
     */
    public void put(Principal principal, List<Permission> permissions) {

        permissionStore.put(principal, permissions);
    }

    /**
     * This function returns the List of permissions associated with the provided principal.
     * If permissions are modified during storage they must be reverted back to their original state during get().
     */
    public List<Permission> get(Principal principal) {

        return permissionStore.get(principal);
    }

    /**
     * This function in-place deletes all targeted permissions associated with a given principal.
     */
    public void delete(Principal principal, List<Permission> permissions) {

        for (Permission permission : permissions) {
            permissionStore.remove(principal, permission);
        }
    }
}
