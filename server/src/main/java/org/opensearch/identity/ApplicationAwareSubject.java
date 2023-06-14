/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.tokens.AuthToken;

import java.security.Principal;
import java.util.Set;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An individual, process, or device that causes information to flow among objects or change to the system state.
 *
 * @opensearch.experimental
 */
public final class ApplicationAwareSubject implements Subject {

    private final Function<Principal, Boolean> checkApplicationExists() {

        return new Function<Principal, Boolean>() {
            @Override
            public Boolean apply(Principal principal) {

                return (IdentityService.getInstance().getApplicationStrings().contains(principal.getName()));
            }
        };
    }

    private final Function<Principal, Set<Scope>> getApplicationScopes() {

        return new Function<Principal, Set<Scope>>() {
            @Override
            public Set<Scope> apply(Principal principal) {
                return IdentityService.getInstance().getApplicationScopes(principal).stream()
                    .map(ApplicationScopes::valueOf)
                    .collect(Collectors.toSet());
            }
        };
    }

    private final Subject wrapped;

    /** Only to be used by IdentityService / tests */
    public ApplicationAwareSubject(final Subject wrapped) {
        this.wrapped = wrapped;
    }

    // Passthroughs for wrapped subject
    public Principal getPrincipal() {
        return wrapped.getPrincipal();
    }
    public void authenticate(final AuthToken token) {
        wrapped.authenticate(token);
    }
    public Optional<Principal> getApplication() {
        return wrapped.getApplication();
    }
    // end Passthroughs for wrapped subject


    /**
     * Checks scopes of the current subject if they are allowed for any of the listed scopes
     * @param scope The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    public boolean isAllowed(final List<Scope> scope) {
        final Optional<Principal> appPrincipal = wrapped.getApplication();
        if (appPrincipal.isEmpty()) {
            // If there is no application, actions are permitted by default
            return true;
        }

        if (!this.checkApplicationExists().apply(appPrincipal.get())) {
            return false;
        }

        final Set<Scope> scopesOfApplication = this.getApplicationScopes().apply(appPrincipal.get());
        if (scopesOfApplication.contains(ApplicationScopes.Trusted_Fully)) {
            // Applications that are fully trusted automatically pass all checks
            return true;
        }

        final List<Scope> matchingScopes = scopesOfApplication.stream().filter(scope::contains).collect(Collectors.toList());
        return !matchingScopes.isEmpty();
    }
}
