/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * An extension of the abstract Permission class which uses String-object Permissions.
 *
 * Example "opensearch.indexing.index.create"
 *
 * @opensearch.experimental
 */
public class Permissions extends Permission {

    private final static String PERMISSION_DELIMITER = "\\.";

    public final static String[] INVALID_CHARACTERS = new String[] { ":", "" }; // This is a placeholder for what may want to be banned

    // This is a placeholder for the different actions which a permission may grant a Subject to perform
    public final static String[] QUALIFIED_ACTIONS = new String[] { "CREATE", "READ", "WRITE", "DELETE", "UPDATE" };

    // A placeholder for the different resources which a permission may grant a permission based on
    public final static String[] QUALIFIED_RESOURCES = new String[] { "index", "indices", "cluster", "all" };

    public String permissionString;

    public void Permission(String permission) {
        this.permissionString = permission;
    }

    /**
     * Check that the permission does not contain any forbidden strings.
     * This set implementation does so in O(n).
     * Assumes that the permission is formatted as <principal>.<resource>.<action>
     * The principal should already be verified before the permission is created.
     */
    public boolean permissionIsValidFormat() {

        ArrayList<String> permissionSegments = new ArrayList<>(Arrays.asList(this.permissionString.split(PERMISSION_DELIMITER)));
        Set<String> permissionSet = new HashSet<>(permissionSegments);
        Set<String> invalidSet = new HashSet<>(new ArrayList<>(Arrays.asList(INVALID_CHARACTERS)));
        Set<String> actionSet = new HashSet<>(new ArrayList<>(Arrays.asList(QUALIFIED_ACTIONS)));
        Set<String> resourceSet = new HashSet<>(new ArrayList<>(Arrays.asList(QUALIFIED_RESOURCES)));
        permissionSet.retainAll(invalidSet);
        if (permissionSet.size() > 0) {
            return false;
        }
        if (!resourceSet.contains(permissionSegments.get(1))) {
            return false;
        }
        if (!actionSet.contains(permissionSegments.get(2))) {
            return false;
        }
        return true;
    }

    public static class InvalidPermissionName extends RuntimeException {
        public InvalidPermissionName(final String name) {
            super("The name '" + name + "' is not a valid permission name");
        }
    }
}
