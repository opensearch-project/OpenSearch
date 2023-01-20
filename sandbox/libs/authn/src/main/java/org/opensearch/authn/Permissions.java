/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import java.util.ArrayList;
import java.util.List;

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

    public String[] permissionSegments;

    public String principal;
    public String resource;

    public String action;

    @Override
    public void Permission(String permission) {

        this.permissionString = permission;
        this.permissionSegments = permissionString.split(PERMISSION_DELIMITER);
        this.principal = permissionSegments[0];
        this.resource = permissionSegments[1];
        this.action = permissionSegments[2];
    }

    /**
     * Check that the permission does not contain any forbidden strings.
     * This set implementation does so in O(n).
     * Assumes that the permission is formatted as <principal>.<resource>.<action>
     * The principal should already be verified before the permission is created.
     */
    @Override
    public boolean isValidFormat() {

        // Check for illegal characters in any of the permission segments O(3n)
        for (int i = 0; i < INVALID_CHARACTERS.length; i++) {
            if (this.principal.contains(INVALID_CHARACTERS[i])
                || this.resource.contains(INVALID_CHARACTERS[i])
                || this.action.contains(INVALID_CHARACTERS[i])) {
                return false;
            }
        }

        // Make sure the resource being acted on is one of the qualified resources
        if (!new ArrayList(List.of(QUALIFIED_RESOURCES)).contains(this.resource.toUpperCase())) {
            return false;
        }

        // Make sure the action being taken is one of the qualified actions
        if (!new ArrayList(List.of(QUALIFIED_ACTIONS)).contains(this.action.toUpperCase())) {
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
