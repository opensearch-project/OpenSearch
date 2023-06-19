/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.action;

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
 * @opensearch.experimental
 */
public enum ActionScope implements Scope {
    ALL(ScopeArea.ALL, "ALL"),
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
