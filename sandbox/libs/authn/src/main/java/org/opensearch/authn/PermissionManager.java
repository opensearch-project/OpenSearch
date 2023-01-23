/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.regex.Pattern;

public class PermissionManager implements PermissionHandler {

    PermissionStorage permissionStorage = new PermissionStorage();

    protected final Logger log = LogManager.getLogger(this.getClass());

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

        log.debug("Permission grant event completed, grant identifier: ", grantIdentifier);

        return grantIdentifier;
    }

    /**
     * Return an ArrayList of all the permissions associated with a single permission grant event
     */
    @Override
    public ArrayList<Permission> getPermissions(String grantIdentifier) {

        log.debug("Checking permissions from grant event: ", grantIdentifier);
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
     */
    public void deletePermissions(String grantIdentifier, String regex) {

        // TODO: There may be a better way to do this.
        ArrayList<Permission> grantedPermissions = permissionStorage.get(grantIdentifier);
        ArrayList<Permission> toDelete = new ArrayList<>();
        for (Permission permission : grantedPermissions) {
            if (Pattern.matches(regex, permission.permissionString)) {
                toDelete.add(permission);
            }
        }
        Object[] toDeleteArray = toDelete.toArray();
        permissionStorage.delete(grantIdentifier, (Permission[]) toDeleteArray);
    }
}
