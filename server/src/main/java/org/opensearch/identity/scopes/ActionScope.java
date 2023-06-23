/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.identity.scopes;

import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.scopes.ScopeEnums.ScopeNamespace;
import org.opensearch.identity.scopes.ScopeEnums.ScopeArea;

/**
 * This enum defines the ActionScope Scope implementation.
 *
 * An ActionScope is composed of a ScopeNamespace, ScopeArea, and Action.
 * Together, these three components reference a Scope which confers execution privileges to a Subject. For example, the READ ActionScope
 * is has a ScopeArea of INDEX and an Action of READ. Its ScopeNamespace is ACTION since it is an ActionScope. This ActionScope is required by all
 * Subjects which seek to perform read-related operations on an INDEX.
 *
 * ActionScopes.ALL: Grants access to execute any REST or Transport action in OpenSearch
 * ActionScopes.READ: Grants access to execute all REST and Transport actions focusing on "read" operations. Specifically GetAction and MultiGetAction
 *
 * @opensearch.experimental
 */
public enum ActionScope implements Scope {
    ALL(ScopeArea.CLUSTER, "ALL"),
    READ(ScopeArea.INDEX, "READ");

    public final ScopeArea area;
    public final String action;

    ActionScope(ScopeArea area, String action) {
        this.area = area;
        this.action = action;
    }

    public ScopeNamespace getNamespace() {
        return ScopeNamespace.ACTION;
    }

    public ScopeArea getArea() {
        return this.area;
    }

    public String getAction() {
        return this.action;
    }
}
