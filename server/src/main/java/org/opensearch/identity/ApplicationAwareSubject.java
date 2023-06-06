/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.opensearch.action.ActionScopes;
import org.opensearch.common.util.set.Sets;
import org.opensearch.identity.tokens.AuthToken;

public final class ApplicationAwareSubject implements Subject {
    private List<Scope> scopes;
    private List<Principal> applications;

    /**
     * Creates a new ApplicationAwareSubject for use with the IdentityPlugin
     * Cannot return null
     * @param subject The name of the subject
     */
    public ApplicationAwareSubject(Subject subject) {
        this.scopes = List.of();
        this.applications = List.of();
    }

    /**
     * Checks scopes of the current subject if they are allowed for any of the listed scopes
     * @param scopes The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    public boolean isAllowed(final List<Scope> scopes) {
        if (scopes.contains(ActionScopes.ALL)) {
            return true;
        }
        Set<Scope> intersection = Sets.intersection(Set.copyOf(this.scopes), Set.copyOf(scopes));
        return intersection.size() > 0;
    }

    /**
     * Sets the scopes of the Subject to the provided list
     * @param scopes The scopes the subject should have
     */
    public void setScopes(List<Scope> scopes) {
        this.scopes = (scopes);
    }

    /**
     * @return The scopes associated with the subject
     */
    public List<Scope> getScopes() {
        return this.scopes;
    }

    @Override
    public Principal getPrincipal() {
        return null;
    }

    @Override
    public void authenticate(AuthToken token) {

    }

    public void setApplications(List<Principal> applications) {
        this.applications = applications;
    }

    public List<Principal> getApplications() {
        return this.applications;
    }
}
