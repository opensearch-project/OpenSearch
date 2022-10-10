/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;
<<<<<<< HEAD
=======

import org.opensearch.authn.StringPrincipal;
>>>>>>> a4c957e6722 (Integrate new changes with existing identity classes)

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
