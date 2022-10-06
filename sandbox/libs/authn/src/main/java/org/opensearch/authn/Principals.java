/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.opensearch.authn.StringPrincipal;

import java.security.Principal;

/**
 * Available OpenSearch internal principals
 *
 * @opensearch.experimental
 */
public enum Principals {

    /**
     * Represents a principal which has not been authenticated
     */
    UNAUTHENTICATED(new StringPrincipal("Unauthenticated"));

    private final Principal principal;

    private Principals(final Principal principal) {
        this.principal = principal;
    }

    /**
     * Returns the underlying principal for this
     */
    public Principal getPrincipal() {
        return principal;
    }

}
