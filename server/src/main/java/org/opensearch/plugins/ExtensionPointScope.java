/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.OpenSearchException;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.scopes.ScopeEnums.ScopeArea;
import org.opensearch.identity.scopes.ScopeEnums.ScopeNamespace;

/**
 * scopes associated with extension points, used by plugins/extensions
 *
 * @opensearch.experimental
 */
public enum ExtensionPointScope implements Scope {
    Action(ScopeArea.EXTENSION_POINT, "ALLOW");

    public final ScopeArea area;
    public final String action;

    ExtensionPointScope(ScopeArea area, String action) {
        this.area = area;
        this.action = action;
    }

    public ScopeNamespace getNamespace() {
        return ScopeNamespace.EXTENSION_POINT;
    }

    public ScopeArea getArea() {
        return this.area;
    }

    public String getAction() {
        return this.action;
    }

    /**
     * Exception raised when an ExtensionPointScope is missing
     *
     * @opensearch.experimental
     */
    public static class ExtensionPointScopeException extends OpenSearchException {
        public ExtensionPointScopeException(final ExtensionPointScope missingScope) {
            super("Missing scope for this extension point " + missingScope.asPermissionString());
        }
    }
}
