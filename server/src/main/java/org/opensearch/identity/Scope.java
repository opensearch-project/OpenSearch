/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

/**
 * Limitation for the scope of an application in OpenSearch
 *
 * @opensearch.experimental
 */
public interface Scope {
    String getNamespace();

    String getArea();

    String getAction();

    default String asPermissionString() {
        return getNamespace() + "." + getArea() + "." + getAction();
    }
}
