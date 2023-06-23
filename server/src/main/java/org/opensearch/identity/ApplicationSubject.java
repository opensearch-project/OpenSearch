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

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.opensearch.cluster.ApplicationManager;
import org.opensearch.identity.scopes.ApplicationScope;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.tokens.AuthToken;

/**
 *
 * @opensearch.experimental
 */
public class ApplicationSubject implements Subject {

    public boolean applicationExists() {
        return (ApplicationManager.getInstance().associatedApplicationExists(wrapped.getPrincipal()));
    }

    private final Subject wrapped;

    public ApplicationSubject(final Subject wrapped) {
        this.wrapped = wrapped;
    }

    public Set<String> getScopes() {
        return ApplicationManager.getInstance().getApplicationScopes(wrapped.getPrincipal());
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
}
