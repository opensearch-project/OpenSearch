/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.action;

import org.opensearch.identity.Scope;

/**
 * Scopes associated with actions in OpenSearch
 *
 * @opensearch.experimental
 */
public enum ActionScopes implements Scope {
    Index_Read(),
    // TODO: Write REST / Transport actions have not been covered
    Index_ReadWrite();

    public String getNamespace() {
        return "Action";
    }

    public String getArea() {
        return name().split("_")[0];
    }

    public String getAction() {
        return name().split("_")[1];
    }
}
