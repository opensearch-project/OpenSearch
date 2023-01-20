/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.apache.commons.lang.RandomStringUtils;

import java.util.ArrayList;

public class PermissionManager implements PermissionHandler {

    PermissionStorage permissionStorage = new PermissionStorage();

    /**
     * This function takes a Permission array and adds it to permission storage.
     * It returns a random ID corresponding to the permission grant event.
     */
    @Override
    public String putPermissions(Permission[] permissions) {

        ArrayList<Permission> toGrant = new ArrayList<>();

        // Check that the permissions are all valid and remove any that are invalid from the grant
        for (Permission permission : permissions) {

            if (permission.isValidFormat()) {
                toGrant.add(permission);
            }
        }

        // When a permission grant event occurs, a unique identifier should be returned as a reference; also usable for logging
        String grantIdentifier = RandomStringUtils.randomAlphanumeric(32);

        permissionStorage.put(grantIdentifier, toGrant);

        return grantIdentifier;
    }

    /**
     * Return an ArrayList of all the permissions associated with a single permission grant event
     */
    @Override
    public ArrayList<Permission> getPermissions(String grantIdentifier) {

        return permissionStorage.get(grantIdentifier);
    }

    /**
     * Delete target permissions associated with a given permission grant event.
     */
    @Override
    public void deletePermissions(String grantIdentifier, Permission[] permissions) {

        permissionStorage.delete(grantIdentifier, permissions);
    }

    /**
     * Delete permissions based off of regex strings.
     * Not fully implemented
     */
    public void deletePermissions(String grantIdentifier, String regex) {

        // TODO: Fully implement this
        permissionStorage.delete(grantIdentifier, regex);
    }
}
