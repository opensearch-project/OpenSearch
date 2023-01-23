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
 * This interface represents the abstract notion of a permission handler. A permission handler needs to be able to service the
 * assignment and verification of permissions.
 *
 * @opensearch.experimental
 */
public interface PermissionHandler {

    // Currently have users --> roles --> permissions want user --> permissions but permissions should be stored elsewhere not directly with
    // the user objects
    // Do not need the higher level construct
    // Want to be able to resolve users to a list of permission
    // Multi-tenancy is like a private index -- not going to be considered for now.
    // Each grant having an ID and have a table then the endpoint has the username and a permission -- this grant could end up as a document
    // in an index

    /**
     * This function grants an array of permission to a subject when provided a permission array and returns a grant identifier representing the permission
     * grant event. It does so by adding the identifier and associated permissions to a permission storage data structure. A valid permission
     * storage structure can be any structure that can hold generic or String-objects and which allows at least O(n) traversal as well as the keying of
     * permissions by identifier strings. For example an OpenSearch index or for small modeling clusters, a HashMap in memory.
     *
     * A SQL-style database solution is possible by storing the identifier and then the separated components of the Permissions.
     *
     * A principal should never be able to grant a permission to itself and implementations should ensure that only valid
     * and permitted Subjects are able to execute this operation.
     */
    public String putPermissions(Permission[] permission);

    /**
     * Returns a list of the permissions granted during a permission grant event.
     * Requires that the grantIdentifier exist, should throw an error if none can be found.
     * Invalid permission checks should be rejected.
     */
    public ArrayList<Permission> getPermissions(String grantIdentifier);

    /**
     * This function in-place deletes all targeted permissions associated with a given permission grant event.
     * This function should be implemented such that '*' means that all permissions associated with the grant event are deleted.
     */
    public void deletePermissions(String permissionIdentifier, Permission[] permissions);

}
