/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.scopes.ScopeEnums.ScopeArea;
import org.opensearch.identity.scopes.ScopeEnums.ScopeNamespace;

/**
 * This enum defines the ApplicationScope implementation of the Scope interface. Like other Scope implementations, ApplicationScope enum constants
 * contain a ScopeNamespace, ScopeArea, and Action.
 *
 * An ApplicationScope dictates special privileges an application may have. For example, the SuperUserAccess ApplicationScope provides an application
 * trust equal to that of an administrator.
 *
 * @opensearch.experimental
 */
public enum ApplicationScope implements Scope {
    SuperUserAccess(ScopeArea.ALL, "ALL");

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
