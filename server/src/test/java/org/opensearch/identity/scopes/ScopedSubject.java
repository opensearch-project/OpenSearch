/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.scopes;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import org.opensearch.identity.ApplicationAwareSubject;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.tokens.AuthToken;

public class ScopedSubject implements ApplicationAwareSubject {

    Set<Scope> scopes;
    Principal principal;

    ScopedSubject(Set<Scope> scopeSets) {
        principal = new NamedPrincipal(this.toString());
        scopes = scopeSets;
    }

    @Override
    public Set<Scope> getScopes() {
        return scopes;
    }

    @Override
    public boolean applicationExists() {
        return false;
    }

    @Override
    public Principal getPrincipal() {
        return principal;
    }

    @Override
    public void authenticate(AuthToken token) {

    }

    @Override
    public Optional<Principal> getApplication() {
        return Optional.empty();
    }
}
