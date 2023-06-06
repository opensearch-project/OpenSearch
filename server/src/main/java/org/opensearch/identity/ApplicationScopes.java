/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

/**
 * Generic Scopes in OpenSearch
 *
 * @opensearch.experimental
 */
public enum ApplicationScopes implements Scope {
    
    Trusted_Fully();

    public String getNamespace() {
        return "Application";
    }

    public String getArea() {
        return name().split("_")[0];
    }

    public String getAction() {
        return name().split("_")[1];
    }
}