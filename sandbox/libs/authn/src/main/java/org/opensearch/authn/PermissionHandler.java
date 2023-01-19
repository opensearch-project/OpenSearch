/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import java.security.Principal;
import java.util.ArrayList;

/**
 * This interface represents the abstract notion of a permission handler. A permission handler needs to be able to service the
 * assignment and
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
     * This function needs to be able to resolve a java Principal to an associated java Subject and return the associated Subject or throw an error if none corresponds
     */
    public Subject resolvePrincipal(Principal principal);

    /**
     * This function grants a permission to a subject when provided the permission and a principal resolvable to a subject
     * A principal should never be able to grant a permission to itself and implementations should ensure that only valid Subjects are able to execute this operation.
     */
    public void putPermission(Permission permission, Principal principal);

    /**
     * Returns a list of the permissions granted to the provided principal.
     * Requires that the principal is resolvable to a Subject, throws an error no corresponding subject can be identified.
     */
    public ArrayList<Permission> getPermission(Principal principal);

    /**
     * This function should be able to in-place remove a permission from the granted permissions of the Subject associated with the provided principal.
     * If no matching permission is found an error should be thrown.
     * If no resolvable Subject is associated with the Principal an error should be thrown.
     */
    public void deletePermission(Permission permission, Principal principal);

}
