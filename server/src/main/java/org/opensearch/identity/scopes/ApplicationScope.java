/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

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
 * ApplicationScope.IMPERSONATE: Allows an application to impersonate users via use of OnBehalfOf Tokens.
 * ApplicationScope.SUPER_USER_ACCESS: Confers "skip check" authority to an application. This allows it to bypass all scope checks.
 * ApplicationScope.SYSTEM_INDICES: Allows an application to create and own a unique system index. This allows for full access to that specific index.
 *
 * @opensearch.experimental
 */
public enum ApplicationScope implements Scope {
    IMPERSONATE(ScopeArea.IMPERSONATE, "USER"),
    SUPER_USER_ACCESS(ScopeArea.SUPER_USER_ACCESS, "ALL"),
    SYSTEM_INDICES(ScopeArea.SYSTEM_INDEX, "OWN");


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
        return this.area;
    }

    public String getAction() {
        return this.action;
    }
}
