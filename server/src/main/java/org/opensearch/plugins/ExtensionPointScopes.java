/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.identity.scopes.Scope;

public enum ExtensionPointScopes implements Scope  {
    ActionPlugin,
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
}