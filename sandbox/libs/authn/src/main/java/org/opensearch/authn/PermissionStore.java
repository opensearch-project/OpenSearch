/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import java.util.ArrayList;

/**
 * This interface represents an abstract permission store. A permission store should be a data structure which
 * supports add, get, and delete operations and can store "String : ArrayList<Permission>" pairs or simulate this storage.
 * The underlying data structure can be options other than a HashMap as long as the implementation
 * has a way to translate between the required functions and the data structure below.
 *
 * For example, an Array-based solution could be implemented by appending the String to the front of every Permission in
 * the ArrayList and then storing the Permissions in a Permission[]. You would also need to implement the add, get, and delete
 * methods to support this structure effectively.
 *
 * @opensearch.experimental
 */
public interface PermissionStore {

    /**
     * This function adds a new grant permissions event to the permissions store. The eventIdentifier is a unique string that
     * represents the event (it does not need to be encrypted and should not be deterministic unless time is a factor of the generation).
     * The ArrayList is a list of all permissions that are being granted during the associated event and should be referencable
     * by the eventIdentifier.
     */
    public void put(String eventIdentifier, ArrayList<Permission> permissions);

    /**
     * This function returns the ArrayList of permissions added to the permission store during the provided permission grant event.
     * If permissions are modified during storage they must be reverted back to their original state during get().
     */
    public ArrayList<Permission> get(String eventIdentifier);

    /**
     * This function in-place deletes all targeted permissions associated with a given permission grant event.
     * This function should be implemented such that '*' means that all permissions associated with the grant event are deleted.
     */
    public void delete(String eventIdentifier, Permission[] permissions);

}
