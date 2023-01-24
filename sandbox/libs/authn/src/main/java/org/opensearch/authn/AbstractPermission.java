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
abstract class AbstractPermission {

    // If using a string for construction, a delimiter is required to split the string
    String PERMISSION_DELIMITER;

    String permissionString;

    // Every permissionString must be resolvable to its constituent parts: <resource>.<action>
    // These are then stored separately to avoid costly String manipulation.
    String resource;

    String action;

}
