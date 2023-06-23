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
import java.util.stream.Collectors;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.ScopeAwareSubject;
import org.opensearch.identity.tokens.AuthToken;

public class ScopedSubject implements ScopeAwareSubject {

    Set<Scope> scopes;
    Principal principal;

    ScopedSubject() {
        principal = new NamedPrincipal("ScopedSubject");
        scopes = Set.of();
    }

    @Override
    public Set<String> getScopes() {
        return scopes.stream().map((scope) -> toString()).collect(Collectors.toSet());
    }

    @Override
    public void setScopes(Set<Scope> scopes) {
        this.scopes = scopes;
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
