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
 * Scopes associated with actions in OpenSearch
 *
 * @opensearch.experimental
 */
public enum ActionScope implements Scope {
    ALL(ScopeArea.ALL, "ALL"),
    CLUSTER_READ(ScopeArea.CLUSTER, "READ"),
    CLUSTER_ALL(ScopeArea.ALL, "ALL"),
    INDEX_READ(ScopeArea.INDEX, "READ"),
    INDEX_READWRITE(ScopeArea.INDEX, "READWRITE"),
    INDEX_SEARCH(ScopeArea.INDEX, "SEARCH"),
    INDEX_ALL(ScopeArea.INDEX, "ALL");

    public final ScopeArea area;
    public final String action;

    ActionScope(ScopeArea area, String action) {
        this.area = area;
        this.action = action;
    }

    public ScopeNamespace getNamespace() {return ScopeNamespace.ACTION;}

    public ScopeArea getArea() {
        return this.area;
    }

    public String getAction() {
        return this.action;
    }
}
