/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

/**
 * This is an abstract class that defines the minimum expectations for a permission object.
 * A permission implementation of this class can implement further functionality or structuring.
 * A permission needs to entail the action it allows and the resource its performed on.
 */
abstract class Permission {

    // If using a string for construction, a delimiter is required to split the string
    String PERMISSION_DELIMITER;

    // If using string-object permissions, you use the invalid characters for ensuring formatting
    String[] INVALID_CHARACTERS;

    // An array of the valid operations which a permission can grant the privilege to perform.
    String[] QUALIFIED_OPERATIONS;

    // An array of the available resources which a permission can grant some operation to act upon.
    String[] QUALIFIED_RESOURCES;

    String permissionString;

    abstract void Permission(String permission);

    abstract boolean permissionIsValidFormat();

}
