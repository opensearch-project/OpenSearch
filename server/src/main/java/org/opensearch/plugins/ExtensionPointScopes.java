/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.OpenSearchException;
import org.opensearch.identity.Scope;

/**
 * scopes associated with extension points, used by plugins/extensions
 * 
 * @opensearch.experimental
 */
public enum ExtensionPointScopes implements Scope  {
    Action,
    // TODO: implement checks for all other scopes
    Analysis,
    CircuitBreaker,
    Cluster,
    Discovery,
    Engine,
    Extensible,
    Identity,
    IndexStore,
    Ingest,
    Mapper,
    Network,
    PersistantTask,
    Reloadable,
    Repository,
    Script,
    SearchPipeline,
    Search,
    SystemIndex;

    public String getNamespace() {
        return "ExtensionPoint";
    }

    public String getArea() {
        return name();
    }

    public String getAction() {
        return "Allowed";
    }

    /**
     * Exception raised when an ExtensionPointScopes is missing
     *
     * @opensearch.experimental
     */
    public static class ExtensionPointScopeException extends OpenSearchException {
        public ExtensionPointScopeException(final ExtensionPointScopes missingScope) {
            super("Missing scope for this extension point " + missingScope.asPermissionString());
        }
    }
}