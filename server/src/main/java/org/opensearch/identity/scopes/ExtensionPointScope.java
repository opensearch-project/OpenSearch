/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import org.opensearch.OpenSearchException;
import org.opensearch.identity.scopes.ScopeEnums.ScopeArea;
import org.opensearch.identity.scopes.ScopeEnums.ScopeNamespace;

/**
 * ExtensionPointScope is an enumerator which implements the Scope interface.
 *
 * An ExtensionPointScope confers the ability of an application to extend any of OpenSearch's extension points. An application lacking the
 * appropriate scope will not be able to extend the actions defined in the interface it extends.
 *
 * ExtensionPointScope.ACTION: Grant access to call the implemented methods of the Action Plugin Interface
 *
 * @opensearch.experimental
 */
public enum ExtensionPointScope implements Scope {
    ACTION(ScopeArea.ACTION_PLUGIN, "IMPLEMENT");

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
