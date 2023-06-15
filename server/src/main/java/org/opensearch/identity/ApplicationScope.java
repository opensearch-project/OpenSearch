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
public enum ApplicationScope implements Scope {
    TRUSTED(ScopeArea.ALL, "TRUSTED"),
    UNTRUSTED(ScopeArea.ALL, "UNTRUSTED");

    public final ScopeArea area;
    public final String action;

    ApplicationScope(ScopeArea area, String action) {
        this.area = area;
        this.action = action;
    }

    public ScopeNamespace getNamespace() {
        return ScopeNamespace.APPLICATION;
    }

    public ScopeArea getArea() {
        return ScopeArea.APPLICATION;
    }

    public String getAction() {
        return this.action;
    }
}
