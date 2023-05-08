/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

/**
 * Any kind of limitation for an extension
 */
public interface Scope {
    String getNamespace();
    String getArea();
    String getAction();
    default String asPermission() {
        return getNamespace() + "." + getAction() + "." + getAction();
    }
}

