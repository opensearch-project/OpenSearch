/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.scopes.ScopeEnums.ScopeArea;
import org.opensearch.identity.scopes.ScopeEnums.ScopeNamespace;

/**
 * Generic Scopes in OpenSearch
 *
 * @opensearch.experimental
 */
public enum ApplicationScopes implements Scope {

    Trusted_Fully();

    public ScopeNamespace getNamespace() {
        return ScopeNamespace.APPLICATION;
    }

    public ScopeArea getArea() {
        return ScopeArea.APPLICATION;
    }

    public String getAction() {
        return name().split("_")[1];
    }
}
