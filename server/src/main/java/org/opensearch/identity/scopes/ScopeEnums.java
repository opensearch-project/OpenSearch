/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

public class ScopeEnums {
    public enum ScopeArea {
        APPLICATION,
        CLUSTER,
        INDEX,
        ALL;
    }

    public enum ScopeNamespace {
        ACTION,
        APPLICATION,
        EXTENSION_POINT;
    }
}
